const MongoClient = require("mongodb").MongoClient;
const got = require("got");
const crypto = require("crypto");

let cachedDB = null;

const MAX_QUERY_RUNNING_SECONDS = process.env["MAX_QUERY_RUNNING_SECONDS"] || 3;
const DEBUG = process.env["DEBUG"] || true;
const MONGO_URI = process.env["MONGODB_URI"] || "mongodb://127.0.0.1:27017";
const DING_TALK_ACCESS_TOKEN = process.env["DING_TALK_ACCESS_TOKEN"];
const DING_TALK_SECRET = process.env["DING_TALK_SECRET"];
const DING_NOTIFICATION_ENABLED =
  process.env["DING_NOTIFICATION_ENABLED"] || false;

function notificationConfigVerified() {
  if (!DING_NOTIFICATION_ENABLED) {
    debugLog(
      "DING_NOTIFICATION_ENABLED is not enabled. Notifications will not be sent."
    );
    return false;
  }
  if (DING_TALK_ACCESS_TOKEN === undefined || DING_TALK_SECRET === undefined) {
    debugLog(
      "DingTalk Bot Credentials are missing. Notifications will not be sent."
    );
    return true;
  }
  return true;
}

async function notifyQueryKilled(message) {
  timestamp = new Date().getTime();
  signature = signSignature(timestamp, DING_TALK_SECRET);
  const searchParams = new URLSearchParams([
    ["access_token", DING_TALK_ACCESS_TOKEN],
    ["timestamp", timestamp],
    ["sign", signature],
  ]);
  const options = {
    headers: {
      "Content-type": "application/json",
      "cache-control": "no-cache",
    },
    pathname: "/robot/send",
    method: "POST",
    searchParams,
    body: JSON.stringify({
      msgtype: "markdown",
      markdown: { title: "Query Killer Response", text: message },
    }),
  };
  apiResponse = await got("https://oapi.dingtalk.com", options);
  debugLog("Response from DingTalk: ", apiResponse.body);
}

function formatMessage(operations, status) {
  return operations
    .map((operation, index) => {
      return `
### Operation ${index + 1}:\n
**Client**: ${operation.client}\n
**Time Running(s)**: ${operation.secs_running}\n
**Command**: ${JSON.stringify(operation.command)}\n
**Killed**: ${status ? "Yes ✅" : "No ❌"}\n`;
    })
    .join("\n");
}

function signSignature(timestamp, secret) {
  return crypto
    .createHmac("sha256", secret)
    .update(Buffer.from([timestamp, secret].join("\n"), "utf-8"))
    .digest("base64")
    .trim();
}

function debugLog(...args) {
  if (DEBUG) {
    console.log(...args);
  }
}

async function connectToMongo(uri) {
  if (cachedDB != null || cachedDB != undefined)
    return Promise.resolve(cachedDB);
  else {
    const client = await MongoClient.connect(uri, {
      useUnifiedTopology: true,
    });
    cachedDB = client.db("admin");
    return cachedDB;
  }
}

async function killQuery(db, ops) {
  successKills = [];
  failedKills = [];
  await Promise.all(
    ops.map(async (op) => {
      debugLog("Killing query with opid: ", op.opid);
      try {
        result = await db.command({ killOp: 1, op: op.opid });
        debugLog("Query kill result ", result);
        successKills.push(op);
      } catch (e) {
        // Swallow error so that other queries can be killed
        console.error("Failed to kill query: ", op, " due to error: ", e);
        failedKills.push(op);
      }
    })
  );
  return {
    successKills,
    failedKills,
  };
}

async function findSlowQueries(db) {
  const operations = await db.command({
    currentOp: 1,
    $and: [
      { op: { $in: ["command", "query"] } },
      { active: true },
      { secs_running: { $gt: MAX_QUERY_RUNNING_SECONDS } },
    ],
  });
  if (operations.inprog.length > 0) {
    debugLog("Long running queries length: ", operations.inprog.length);
    return operations.inprog;
  }
  return null;
}

exports.handler = async function (_event, context) {
  context.callbackWaitsForEmptyEventLoop = false;
  try {
    const db = await connectToMongo(MONGO_URI);
    const operations = await findSlowQueries(db);
    if (operations === null) {
      return;
    }
    const killResp = await killQuery(db, operations);
    if (notificationConfigVerified()) {
      const message = "# Query Killer Summary💡".concat(
        "\n",
        formatMessage(killResp.successKills, true),
        formatMessage(killResp.failedKills, false)
      );
      await notifyQueryKilled(message);
    }
  } catch (e) {
    console.error(e);
    return e;
  }
};
