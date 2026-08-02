# MongoDB Atlas — `$lookup` pagination performance

Reproduction and fix for the two problems named in Upwork job `~022067836771266137767`:

> *"a paginated listing endpoint that does a `$lookup` over a large (millions of docs)
> collection then filters post-join, and an unindexed regex/text search that scans
> the collection"* — *"some list/search endpoints that take minutes instead of milliseconds"*

Built as a standalone reproduction so the analysis can be shown before any access
to the client's cluster is granted.

---

## The actual constraint

The client runs **~30 GB on an Atlas M20, which has 4 GB of RAM** — a working set
roughly **7× larger than cache**. That single fact explains every symptom they listed:

- a query that examines millions of documents cannot be served from cache
- the misses become disk reads, which is the CPU spike
- sustained pressure stalls the primary long enough to trigger **replica-set elections**

So the fix is **making queries touch fewer documents**, not buying more RAM. They
already said they want to stay on M20; that is the right instinct and this repo
demonstrates it is achievable.

## The scale model

Reproducing 30 GB on a laptop is unnecessary. What has to be faithful is the
**ratio**, so:

| | Client (M20) | This reproduction |
|---|---|---|
| Data | ~30 GB | ~1.3 GB |
| WiredTiger cache | 4 GB | 0.25 GB |
| **Ratio** | **~7:1** | **~5:1** |

Same eviction behaviour, same disk-read penalty, on a laptop.

---

## Running it

```bash
# 1. Start MongoDB 7 with a large cache for fast seeding
CACHE_GB=4 docker compose up -d

# 2. Seed ~4M orders / 2M customers (~2 min)
cd bench && npm install
CUSTOMERS=2000000 ORDERS=4000000 node seed.js

# 3. Denormalise + build the ESR compound index
node migrate-denormalise.js

# 4. Shrink cache to create realistic memory pressure, then measure
cd .. && CACHE_GB=0.25 docker compose up -d && sleep 10
cd bench && node benchmark.js          # -> ../results/results.md

# 5. Same four strategies in Spring Data MongoDB (their stack)
cd ../spring
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 mvn -q spring-boot:run
```

> **JAVA_HOME note.** This laptop has JDKs 17 and 21 installed and `JAVA_HOME` is
> unset, so Maven's compiler plugin resolves to a javac older than the reported
> runtime and fails with *"release version 21 not supported"*. Pinning `JAVA_HOME`
> and targeting 17 fixes it — and 17 is the better target anyway, since Spring
> Boot 3.3 baselines there and the client's runtime version is unknown.

---

## The four strategies

All answer the same question: *page N of COMPLETED orders belonging to gold-tier
EU customers, newest first, with the customer name.*

### A — the anti-pattern (what they have)

```js
[ { $lookup: { from: "customers", localField: "customerId", … } },
  { $unwind: "$customer" },
  { $match: { "customer.region": "EU", "customer.tier": "gold" } },   // ← after the join
  { $sort: { createdAt: -1 } }, { $skip: 10000 }, { $limit: 20 } ]
```

Joins **every** order, then discards ~98%. `$skip` makes the server generate and
throw away 10,000 rows to reach page 500. Cost scales with **collection size and
page depth**, not page size — which is exactly why deep pages take minutes.

No index can rescue this. You cannot index a field that does not exist on the
collection being scanned.

### B — reverse the join

Resolve the selective customer predicate first against an indexed `customers`
query, then drive orders from that id set. Effective while the customer slice is
small; degrades as the `$in` array grows. Included because it is the honest
zero-migration option.

### C — denormalise + ESR index + keyset pagination ← **the recommendation**

Copy `region` and `tier` onto the order at write time, then one index serves
everything:

```
{ status: 1, customerRegion: 1, customerTier: 1, createdAt: -1 }
   \__________ Equality __________/  \___ Sort / Range ___/
```

Pagination becomes a **range cursor on the indexed sort key**, not `$skip`, so
page 1 and page 5,000 cost the same.

**The trade-off, stated plainly:** this creates a write-side consistency
obligation. If a customer changes tier, historical orders must be backfilled — or
accepted as point-in-time, which for an order record is usually the correct
business answer anyway. That is a conversation to have with them, not a decision
to make silently.

### D — the regex

`/term/` is unanchored, so no index can serve it: every document is read and the
pattern applied (COLLSCAN). `/^term/` on an indexed field becomes a b-tree range
scan (IXSCAN).

Atlas Search is the third option and is genuinely better for substring and fuzzy
matching — but it is a separately billed service. **Measure before recommending
it**; an anchored index is free and may be sufficient.

---

## What to read

- `results/results.md` — the before/after table
- `bench/lib/pipelines.js` — the four pipelines, commented
- `spring/…/OrderListingAggregations.java` — the same four in Spring Data
- `docs/findings-report.md` — the client-facing writeup

## Scope note

The job posts a $100 fixed budget against a scope covering profiling, index
redesign, aggregation restructuring, connection-pool tuning, working-set analysis,
retention policy, alerting and a runbook — realistically 20+ hours.

This repo covers the highest-value slice: **diagnosis and the fix for the two
named bottlenecks**. Proposal is phased accordingly.
