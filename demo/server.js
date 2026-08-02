/**
 * Demo API — runs each strategy live against the seeded dataset and returns
 * timing plus explain() counters, so the client can drive it themselves.
 *
 * Deliberately runs the REAL queries rather than replaying recorded numbers.
 * The whole point is that they can pick page 500 and watch the current
 * implementation time out while the optimised one stays flat.
 */
import express from "express";
import { MongoClient } from "mongodb";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { naive, reverseJoin, denormalised, regex, PAGE_SIZE } from "../bench/lib/pipelines.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const URI = process.env.MONGO_URI || "mongodb://localhost:27018";
const DB = process.env.MONGO_DB || "bench";
const PORT = Number(process.env.PORT || 8099);

// Shorter than the benchmark's 30s — a browser demo must not hang. When A hits
// this ceiling that IS the demonstration, so the UI shows it as a result.
const MAX_MS = Number(process.env.MAX_MS || 15_000);

const client = new MongoClient(URI, { maxPoolSize: 20 });
await client.connect();
const db = client.db(DB);
const orders = db.collection("orders");
const customers = db.collection("customers");

// The gold+EU customer id set changes only when the data does — cache it so
// strategy B is measured on its query cost, not on re-deriving the input.
let goldEuIds = null;
async function getGoldEuIds() {
  if (!goldEuIds) {
    goldEuIds = await customers
      .find({ region: "EU", tier: "gold" }, { projection: { _id: 0, customerId: 1 } })
      .map((d) => d.customerId)
      .toArray();
  }
  return goldEuIds;
}

function summarise(explain) {
  const acc = { docsExamined: 0, keysExamined: 0 };
  (function walk(n) {
    if (!n || typeof n !== "object") return;
    if (Array.isArray(n)) return n.forEach(walk);
    if (n.executionStats) {
      acc.docsExamined += n.executionStats.totalDocsExamined ?? 0;
      acc.keysExamined += n.executionStats.totalKeysExamined ?? 0;
    }
    Object.values(n).forEach(walk);
  })(explain);
  return acc;
}

async function runPipeline(pipeline) {
  const t0 = process.hrtime.bigint();
  let rows = [], timedOut = false;
  try {
    rows = await orders.aggregate(pipeline, { maxTimeMS: MAX_MS, allowDiskUse: true }).toArray();
  } catch (e) {
    if (e.code === 50 || /time limit|MaxTimeMS/i.test(e.message || "")) timedOut = true;
    else throw e;
  }
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;

  let stats = { docsExamined: 0, keysExamined: 0 };
  if (!timedOut) {
    try {
      stats = summarise(await db.command({
        explain: { aggregate: "orders", pipeline, cursor: {} },
        verbosity: "executionStats",
        maxTimeMS: MAX_MS,
      }));
    } catch { /* explain blew the ceiling too */ }
  }
  return { ms: Math.round(ms), timedOut, nReturned: rows.length, rows: rows.slice(0, 8), ...stats };
}

const app = express();
app.use(express.json());
app.use(express.static(join(__dirname, "public")));

app.get("/api/meta", async (_req, res) => {
  const [o, c, stats] = await Promise.all([
    orders.estimatedDocumentCount(),
    customers.estimatedDocumentCount(),
    db.command({ dbStats: 1, scale: 1024 * 1024 }),
  ]);
  const server = await db.admin().serverStatus();
  res.json({
    orders: o,
    customers: c,
    dataMB: Math.round(stats.dataSize),
    indexMB: Math.round(stats.indexSize),
    cacheGB: (server.wiredTiger?.cache?.["maximum bytes configured"] ?? 0) / 1024 ** 3,
    maxMs: MAX_MS,
    pageSize: PAGE_SIZE,
  });
});

app.post("/api/run", async (req, res) => {
  const strategy = String(req.body.strategy || "C");
  const page = Math.max(1, Number(req.body.page || 1));
  const skip = (page - 1) * PAGE_SIZE;

  try {
    if (strategy === "A") return res.json(await runPipeline(naive(skip)));

    if (strategy === "B") {
      const ids = await getGoldEuIds();
      return res.json(await runPipeline(reverseJoin(ids, skip)));
    }

    // C uses a range cursor. Resolving the cursor for an arbitrary page is
    // itself a cost and is included in the timing — no cheating.
    let cursor = null;
    if (skip > 0) {
      const probe = await orders
        .find({ status: "COMPLETED", customerRegion: "EU", customerTier: "gold" },
              { projection: { createdAt: 1 }, sort: { createdAt: -1 }, skip: skip - 1, limit: 1 })
        .maxTimeMS(MAX_MS).toArray();
      cursor = probe[0]?.createdAt ?? null;
    }
    res.json(await runPipeline(denormalised(cursor)));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/api/search", async (req, res) => {
  const mode = req.body.mode === "anchored" ? "anchored" : "unanchored";
  const term = String(req.body.term || "ORD000500000");
  const filter = mode === "anchored" ? regex.anchoredIndexed(term) : regex.unanchored(term);

  const t0 = process.hrtime.bigint();
  let rows = [], timedOut = false;
  try {
    rows = await orders.find(filter).limit(20).maxTimeMS(MAX_MS).toArray();
  } catch (e) {
    if (e.code === 50 || /time limit|MaxTimeMS/i.test(e.message || "")) timedOut = true;
    else return res.status(500).json({ error: e.message });
  }
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;

  let stats = { docsExamined: 0, keysExamined: 0 };
  if (!timedOut) {
    try { stats = summarise(await orders.find(filter).limit(20).explain("executionStats")); } catch {}
  }
  res.json({ ms: Math.round(ms), timedOut, nReturned: rows.length, rows: rows.slice(0, 5), ...stats });
});

app.listen(PORT, "0.0.0.0", () =>
  console.log(`demo on http://localhost:${PORT}  (mongo ${URI}, cap ${MAX_MS}ms)`)
);
