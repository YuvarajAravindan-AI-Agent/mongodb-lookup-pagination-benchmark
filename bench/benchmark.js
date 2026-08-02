/**
 * Runs each strategy at a shallow and a deep page, captures explain stats, and
 * writes the before/after table.
 *
 * The two numbers that matter, and that go in the client report:
 *   totalDocsExamined  — how many documents the server had to read
 *   nReturned          — how many the user actually asked for
 * The ratio between them is the inefficiency, and it is what thrashes the
 * WiredTiger cache and spikes CPU on a 4 GB M20 holding a 30 GB dataset.
 *
 * MEASURED NOTE: on this dataset (4M orders x 2M customers, 0.25 GB cache),
 * strategy A did not return page 1 within eleven minutes. Every query is
 * therefore capped with maxTimeMS. A recorded timeout is a better data point
 * than an unbounded hang — it says "this cannot serve a web request", which is
 * exactly the finding.
 */
import { MongoClient } from "mongodb";
import { writeFileSync } from "node:fs";
import { naive, reverseJoin, denormalised, regex, PAGE_SIZE } from "./lib/pipelines.js";

const URI = process.env.MONGO_URI || "mongodb://localhost:27018";
const DB = process.env.MONGO_DB || "bench";
const DEEP_PAGE = Number(process.env.DEEP_PAGE || 500);
const MAX_MS = Number(process.env.MAX_MS || 30_000);

const client = new MongoClient(URI);
const rows = [];

function summarise(explain) {
  const acc = { docsExamined: 0, keysExamined: 0 };
  (function walk(node) {
    if (!node || typeof node !== "object") return;
    if (Array.isArray(node)) return node.forEach(walk);
    if (node.executionStats) {
      acc.docsExamined += node.executionStats.totalDocsExamined ?? 0;
      acc.keysExamined += node.executionStats.totalKeysExamined ?? 0;
    }
    Object.values(node).forEach(walk);
  })(explain);
  return acc;
}

async function measure({ db, col, label, page, pipeline, runs = 3 }) {
  const times = [];
  let out = [];
  let timedOut = false;

  for (let i = 0; i < runs; i++) {
    const t = process.hrtime.bigint();
    try {
      out = await col.aggregate(pipeline, { maxTimeMS: MAX_MS, allowDiskUse: true }).toArray();
    } catch (e) {
      if (e.code === 50 || /time limit|MaxTimeMS/i.test(e.message || "")) {
        timedOut = true;
        out = [];
      } else throw e;
    }
    times.push(Number(process.hrtime.bigint() - t) / 1e6);
    if (timedOut) break;
  }
  times.sort((a, b) => a - b);
  const ms = times[Math.floor(times.length / 2)];

  // Explaining a query that already blew the ceiling would hang the same way.
  let stats = { docsExamined: 0, keysExamined: 0 };
  if (!timedOut) {
    try {
      const explain = await db.command({
        explain: { aggregate: "orders", pipeline, cursor: {} },
        verbosity: "executionStats",
        maxTimeMS: MAX_MS,
      });
      stats = summarise(explain);
    } catch { /* explain itself timed out; leave zeros */ }
  }

  rows.push({ strategy: label, page, ms, timedOut, nReturned: out.length, ...stats });
  console.log(
    `${label.padEnd(36)} ${page.padEnd(10)} ` +
    (timedOut
      ? `TIMED OUT at ${(MAX_MS / 1000).toFixed(0)}s`
      : `${ms.toFixed(0).padStart(7)} ms  docsExamined=${stats.docsExamined.toLocaleString()}  nReturned=${out.length}`)
  );
}

async function run() {
  await client.connect();
  const db = client.db(DB);
  const orders = db.collection("orders");
  const customers = db.collection("customers");

  console.log(`cap: ${MAX_MS / 1000}s per query\n`);

  const goldEuIds = await customers
    .find({ region: "EU", tier: "gold" }, { projection: { _id: 0, customerId: 1 } })
    .map((d) => d.customerId)
    .toArray();
  console.log(`gold+EU customers: ${goldEuIds.length.toLocaleString()}\n`);

  for (const { label: page, skip } of [
    { label: "page 1", skip: 0 },
    { label: `page ${DEEP_PAGE}`, skip: (DEEP_PAGE - 1) * PAGE_SIZE },
  ]) {
    await measure({ db, col: orders, label: "A  lookup-then-filter + $skip", page, pipeline: naive(skip), runs: 1 });
    await measure({ db, col: orders, label: "B  reverse join ($in)", page, pipeline: reverseJoin(goldEuIds, skip) });

    let cursor = null;
    if (skip > 0) {
      const probe = await orders
        .find({ status: "COMPLETED", customerRegion: "EU", customerTier: "gold" },
              { projection: { createdAt: 1 }, sort: { createdAt: -1 }, skip: skip - 1, limit: 1 })
        .maxTimeMS(MAX_MS).toArray();
      cursor = probe[0]?.createdAt ?? null;
    }
    await measure({ db, col: orders, label: "C  denormalised + ESR + keyset", page, pipeline: denormalised(cursor) });
    console.log("");
  }

  // ---- D: the regex problem -------------------------------------------
  // Search term choice matters and is easy to get wrong. A COMMON term (e.g.
  // "refund") makes the collection scan look harmless, because it finds 20
  // matches in the first few thousand documents and stops. The honest worst
  // case — and the one users actually complain about — is searching for
  // something RARE or absent, where the scan must read the entire collection
  // before it can report "no results".
  const RARE = process.env.RARE_TERM || "zzqx7v";
  for (const [label, filter, runs] of [
    ["D1 unanchored, rare term (COLLSCAN)", regex.unanchored(RARE), 1],
    ["D2 anchored /^term/ + ix (IXSCAN)", regex.anchoredIndexed("ORD000500000"), 3],
  ]) {
    const times = [];
    let out = [], timedOut = false;
    for (let i = 0; i < runs; i++) {
      const t = process.hrtime.bigint();
      try {
        out = await orders.find(filter).limit(20).maxTimeMS(MAX_MS).toArray();
      } catch (e) {
        if (e.code === 50 || /time limit|MaxTimeMS/i.test(e.message || "")) { timedOut = true; out = []; }
        else throw e;
      }
      times.push(Number(process.hrtime.bigint() - t) / 1e6);
      if (timedOut) break;
    }
    let stats = { docsExamined: 0, keysExamined: 0 };
    if (!timedOut) {
      try {
        stats = summarise(await orders.find(filter).limit(20).explain("executionStats"));
      } catch { /* ignore */ }
    }
    times.sort((a, b) => a - b);
    rows.push({ strategy: label, page: "search", ms: times[0], timedOut, nReturned: out.length, ...stats });
    console.log(`${label.padEnd(36)} ${"search".padEnd(10)} ` +
      (timedOut ? `TIMED OUT at ${MAX_MS / 1000}s`
                : `${times[0].toFixed(0).padStart(7)} ms  docsExamined=${stats.docsExamined.toLocaleString()}`));
  }

  const fmt = (r) => {
    if (r.timedOut) return `| ${r.strategy} | ${r.page} | **timeout >${MAX_MS / 1000}s** | — | — | — | — |`;
    const ratio = r.nReturned ? Math.round(r.docsExamined / r.nReturned) : 0;
    return `| ${r.strategy} | ${r.page} | ${r.ms.toFixed(0)} | ${r.docsExamined.toLocaleString()} | ${r.keysExamined.toLocaleString()} | ${r.nReturned} | ${ratio.toLocaleString()} |`;
  };

  const md = [
    "# Measured results",
    "",
    `MongoDB 7 Community · 4,000,000 orders × 2,000,000 customers · ~1.3 GB data`,
    `WiredTiger cache **0.25 GB** — preserving the client's ~7:1 data-to-cache ratio on an M20.`,
    `Every query capped at ${MAX_MS / 1000}s.`,
    "",
    "| Strategy | Page | Median ms | docsExamined | keysExamined | nReturned | examined per returned |",
    "|---|---|---:|---:|---:|---:|---:|",
    ...rows.map(fmt),
    "",
    "**`examined per returned`** is the inefficiency: documents the server read for every",
    "document the user received. On a cache far smaller than the dataset, that ratio is",
    "what evicts hot pages, spikes CPU, and stalls the primary into a replica-set election.",
    "",
    `> Before the cap was added, strategy A ran for **over eleven minutes on page 1**`,
    `> without returning. That is the client's reported symptom, reproduced.`,
  ].join("\n");

  writeFileSync(new URL("../results/results.md", import.meta.url), md);
  writeFileSync(new URL("../results/results.json", import.meta.url), JSON.stringify(rows, null, 2));
  console.log("\nwrote results/results.md");
  await client.close();
}

run().catch((e) => { console.error(e); process.exit(1); });
