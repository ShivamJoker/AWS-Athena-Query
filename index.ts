import {
  AthenaClient,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  QueryExecutionStatus,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";

const client = new AthenaClient({
  region: "ap-southeast-1",
  credentials: {
    accessKeyId: "not-to-be-seen",
    secretAccessKey: "definitely-not-to-be-seen",
  },
});

const queryCmd = new StartQueryExecutionCommand({
  QueryString: `
SELECT attributes FROM "analytics_firehose" WHERE event_type = 'song_opened'  
-- WHERE NOT(metrics.secondslistened = 0) AND event_type = 'song_played' AND attributes.songId = '01G6Q25JDHM0R679X2YNZVPTDX' 
-- GROUP BY attributes.songId
`,
  QueryExecutionContext: {
    Database: "analytics",
  },
  ResultConfiguration: {
    OutputLocation: "s3://your-bucket/",
  },
});

const sleep = (delay: number) =>
  new Promise((resolve) => setTimeout(resolve, delay));

const maxPollCount = 100;

async function* pollGen(
  queryCmd: GetQueryExecutionCommand
): AsyncGenerator<QueryExecutionStatus, void, void> {
  for (let i = 0; i < maxPollCount; i++) {
    const res = await client.send(queryCmd);
    const status = res.QueryExecution?.Status;
    const QueryState = res.QueryExecution?.Status?.State;

    if (!status) {
      throw Error("No status present in response");
    }
    yield status;

    const isPending = QueryState == "RUNNING" || QueryState == "QUEUED";
    if (!isPending) break;

    if (i == maxPollCount) {
      throw Error("Query is taking too long to finish!");
    }
  }
}

const getQueryResult = async (QueryExecutionId: string) => {
  const getQueryStatus = new GetQueryExecutionCommand({
    QueryExecutionId,
  });

  const polled = pollGen(getQueryStatus);
  for await (const res of polled) {
    await sleep(100);
    console.dir(res.State, { depth: null });
  }
  const getQueryResCmd = new GetQueryResultsCommand({ QueryExecutionId });
  const queryRes = await client.send(getQueryResCmd);
  return queryRes;
};

const execQuery = async () => {
  console.time("took");
  const { QueryExecutionId } = await client.send(queryCmd);
  const res = await getQueryResult(QueryExecutionId!);

  console.timeEnd("took");
  const rows = res.ResultSet?.Rows;

  // if rows is empty or undefined
  if (!rows || !rows?.length) {
    throw Error("No result found");
  }
  const columns = rows.splice(0, 1)[0].Data;

  const columnNames = columns?.map(({ VarCharValue }) => VarCharValue);

  const rowVals = rows.map(({ Data }) => Data?.map((val) => val.VarCharValue));
  console.table([columnNames, ...rowVals]);
};

execQuery();
