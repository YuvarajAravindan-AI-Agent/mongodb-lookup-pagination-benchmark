/**
 * Seeds a dataset shaped like the client's: a large "join target" collection and
 * an orders collection that references it.
 *
 * Sizing is deliberate. The point is NOT to be big for its own sake — it is to
 * make the working set exceed the 4 GB WiredTiger cache, so that examining
 * documents costs real disk reads, exactly as it does on their M20.
 *
 *   node seed.js                 # default: 2,000,000 customers / 1,000,000 orders
 *   CUSTOMERS=5000000 ORDERS=2000000 node seed.js
 */
import { MongoClient } from "mongodb";

const URI = process.env.MONGO_URI || "mongodb://localhost:27018";
const DB = process.env.MONGO_DB || "bench";
const N_CUSTOMERS = Number(process.env.CUSTOMERS || 2_000_000);
const N_ORDERS = Number(process.env.ORDERS || 1_000_000);
const BATCH = 10_000;

const REGIONS = ["EU", "US", "APAC", "LATAM", "MEA"];
const TIERS = ["bronze", "silver", "gold", "platinum"];
const STATUSES = ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"];

// Weighted so that the target slice (EU + gold + COMPLETED) is ~2% of orders.
// A selective predicate is the realistic case and it is what makes the naive
// pipeline look so bad: it materialises everything, then throws 98% away.
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const weightedRegion = () => (Math.random() < 0.25 ? "EU" : pick(REGIONS));
const weightedTier = () => (Math.random() < 0.20 ? "gold" : pick(TIERS));
const weightedStatus = () => (Math.random() < 0.45 ? "COMPLETED" : pick(STATUSES));

const WORDS = [
  "urgent", "refund", "damaged", "delayed", "gift", "fragile", "priority",
  "backorder", "replacement", "warranty", "invoice", "duplicate",
];
const note = () =>
  Array.from({ length: 6 }, () => pick(WORDS)).join(" ") + " " + Math.random().toString(36).slice(2, 8);

async function seedCustomers(db) {
  const col = db.collection("customers");
  await col.drop().catch(() => {});
  process.stdout.write(`customers: seeding ${N_CUSTOMERS.toLocaleString()} `);

  for (let i = 0; i < N_CUSTOMERS; i += BATCH) {
    const docs = [];
    for (let j = 0; j < BATCH && i + j < N_CUSTOMERS; j++) {
      const id = i + j;
      docs.push({
        customerId: id,
        name: `Customer ${id}`,
        email: `c${id}@example.com`,
        region: weightedRegion(),
        tier: weightedTier(),
        active: Math.random() < 0.9,
      });
    }
    await col.insertMany(docs, { ordered: false });
    if ((i / BATCH) % 20 === 0) process.stdout.write(".");
  }
  console.log(" done");

  // The only index the reverse-join strategy needs on this side.
  await col.createIndex({ customerId: 1 }, { unique: true, name: "customerId_1" });
  await col.createIndex({ region: 1, tier: 1 }, { name: "region_1_tier_1" });
  console.log("customers: indexes built");
}

async function seedOrders(db) {
  const col = db.collection("orders");
  await col.drop().catch(() => {});
  process.stdout.write(`orders:    seeding ${N_ORDERS.toLocaleString()} `);

  // Spread createdAt over two years so the sort key has real cardinality.
  const start = Date.UTC(2024, 0, 1);
  const span = Date.UTC(2026, 0, 1) - start;

  for (let i = 0; i < N_ORDERS; i += BATCH) {
    const docs = [];
    for (let j = 0; j < BATCH && i + j < N_ORDERS; j++) {
      const id = i + j;
      const customerId = Math.floor(Math.random() * N_CUSTOMERS);
      docs.push({
        orderNumber: `ORD-${String(id).padStart(9, "0")}`,
        customerId,
        status: weightedStatus(),
        totalAmount: Math.round(Math.random() * 500_00) / 100,
        createdAt: new Date(start + Math.random() * span),
        notes: note(),
        // searchKey exists purely to demonstrate the anchored-regex fix.
        searchKey: `ORD${String(id).padStart(9, "0")}`,
        items: [{ sku: `SKU-${id % 5000}`, qty: 1 + (id % 4) }],
        // NOTE: customerRegion / customerTier are deliberately NOT written here.
        // They are added by the denormalisation migration so the before/after is
        // an honest demonstration of a migration, not a rigged seed.
      });
    }
    await col.insertMany(docs, { ordered: false });
    if ((i / BATCH) % 20 === 0) process.stdout.write(".");
  }
  console.log(" done");

  // Baseline indexes: what a reasonable team would already have.
  // Note there is NO index that serves the naive pipeline's post-join filter —
  // because no such index can exist. That is the point.
  await col.createIndex({ customerId: 1 }, { name: "customerId_1" });
  await col.createIndex({ createdAt: -1 }, { name: "createdAt_-1" });
  console.log("orders:    baseline indexes built");
}

async function main() {
  const client = new MongoClient(URI);
  await client.connect();
  const db = client.db(DB);
  const t0 = Date.now();

  await seedCustomers(db);
  await seedOrders(db);

  const stats = await db.command({ dbStats: 1, scale: 1024 * 1024 });
  console.log(
    `\nseeded in ${((Date.now() - t0) / 1000).toFixed(1)}s — ` +
      `data ${stats.dataSize.toFixed(0)} MB, indexes ${stats.indexSize.toFixed(0)} MB`
  );
  console.log("next: node migrate-denormalise.js  then  node benchmark.js");
  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
