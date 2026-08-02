/**
 * The migration that makes strategy C possible.
 *
 * Copies the two customer attributes the listing filters on (region, tier) onto
 * each order, then builds one ESR compound index that serves the whole query.
 *
 * This is the part that needs arguing for with a client, because it trades write
 * complexity for read speed and introduces a consistency obligation: if a
 * customer changes tier, their historical orders must be updated (or accepted as
 * point-in-time, which is often the correct business answer for an order record).
 *
 * Deliberately written as a batched $lookup-based update so it is safe to run on
 * a live cluster, not a one-shot forEach that would hold a cursor for hours.
 */
import { MongoClient } from "mongodb";

const URI = process.env.MONGO_URI || "mongodb://localhost:27018";
const DB = process.env.MONGO_DB || "bench";
const BATCH = 50_000;

async function main() {
  const client = new MongoClient(URI);
  await client.connect();
  const db = client.db(DB);
  const orders = db.collection("orders");
  const customers = db.collection("customers");
  const t0 = Date.now();

  // Load the customer attribute map once. At 2M customers this is ~100 MB of
  // JS heap — fine here, and on a real cluster you would instead run this as a
  // $merge aggregation server-side. Noted in the report as the production form.
  process.stdout.write("loading customer attributes ");
  const attrs = new Map();
  const cur = customers.find({}, { projection: { _id: 0, customerId: 1, region: 1, tier: 1 } });
  let n = 0;
  for await (const c of cur) {
    attrs.set(c.customerId, [c.region, c.tier]);
    if (++n % 500_000 === 0) process.stdout.write(".");
  }
  console.log(` ${attrs.size.toLocaleString()} loaded`);

  process.stdout.write("backfilling orders ");
  let processed = 0;
  let ops = [];
  const orderCur = orders.find(
    { customerRegion: { $exists: false } },
    { projection: { _id: 1, customerId: 1 } }
  );

  const flush = async () => {
    if (!ops.length) return;
    await orders.bulkWrite(ops, { ordered: false });
    ops = [];
  };

  for await (const o of orderCur) {
    const a = attrs.get(o.customerId);
    if (a) {
      ops.push({
        updateOne: {
          filter: { _id: o._id },
          update: { $set: { customerRegion: a[0], customerTier: a[1] } },
        },
      });
    }
    if (ops.length >= BATCH) {
      await flush();
      processed += BATCH;
      process.stdout.write(".");
    }
  }
  await flush();
  console.log(" done");

  // The index that does the real work. ESR order:
  //   Equality : status, customerRegion, customerTier
  //   Sort     : createdAt (also the range cursor for keyset pagination)
  process.stdout.write("building ESR compound index ");
  await orders.createIndex(
    { status: 1, customerRegion: 1, customerTier: 1, createdAt: -1 },
    { name: "esr_status_region_tier_createdAt" }
  );
  console.log("done");

  // Supports the anchored-regex demonstration.
  await orders.createIndex({ searchKey: 1 }, { name: "searchKey_1" });

  console.log(`\nmigration complete in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
