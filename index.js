const MongoClient = require("mongodb").MongoClient;

let cachedClient = null;

const MAX_QUERY_RUNNING_SECONDS =
  process.env["MAX_QUERY_RUNNING_SECONDS"] || 10;
const DEBUG = process.env["DEBUG"] || true;
const uri = process.env["MONGODB_URI"] || "mongodb://127.0.0.1:27017";

function debugLog(...args) {
  if (DEBUG) {
    console.log(...args);
  }
}

async function connectToMongo(uri) {
  if (cachedClient != null || cachedClient != undefined)
    return Promise.resolve(cachedClient);
  else {
    const client = await MongoClient.connect(uri, {
      useUnifiedTopology: true,
    });
    cachedClient = client;
    return cachedClient;
  }
}

async function killQuery(db, ops) {
  await Promise.all(
    ops.map(async (op) => {
      debugLog("Killing query with opid: ", op.opid);
      try {
        result = await db.command({ killOp: 1, op: op.opid });
        debugLog("Query kill result ", result);
      } catch (e) {
        // Swallow error so that other queries can be killed
        console.error("Failed to kill query: ", op, " due to error: ", e);
      }
    })
  );
}

async function findSlowQueries(client) {
  const db = client.db("admin");
  const operations = await db.command({ currentOp: 1 });
  const longRunningQueryOps = operations.inprog.filter((el) => {
    return (
      (el.op === "command" || el.op === "query") &&
      el.secs_running >= MAX_QUERY_RUNNING_SECONDS
    );
  });
  if (longRunningQueryOps.length > 0) {
    debugLog("Long running queries length: ", longRunningQueryOps.length);
    await killQuery(db, longRunningQueryOps);
  }
}

exports.handler = async function (_event, context) {
  context.callbackWaitsForEmptyEventLoop = false;
  try {
    client = await connectToMongo(uri);
    killResp = await findSlowQueries(client);
  } catch (e) {
    console.error(e);
    return e;
  }
};
