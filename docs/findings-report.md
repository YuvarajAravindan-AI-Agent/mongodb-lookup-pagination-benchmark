# MongoDB Atlas M20 — Findings and Recommendations

**Prepared for:** [client]
**Scope:** paginated listing endpoint and unindexed search on production Atlas (M20, ~30 GB)
**Method:** reproduced independently on MongoDB 7 Community, WiredTiger cache constrained
to preserve the client's ~7:1 data-to-cache ratio.

> Measured numbers are in `results/results.md`. This document explains what they mean
> and what to do about them.

---

## 1. The single constraint that explains every symptom

An M20 has **4 GB of RAM**. The dataset is **~30 GB**. The working set therefore cannot
be cache-resident, and that one fact produces all four reported symptoms:

| Reported symptom | Mechanism |
|---|---|
| Slow queries and heavy aggregations | Every document examined that is not cached is a disk read |
| Occasional CPU spikes | Cache eviction plus disk I/O plus in-memory sorting |
| Brief replica-set elections | A saturated primary misses heartbeat deadlines |
| List endpoints taking minutes | `$skip` cost grows linearly with page depth |

**This is not a hardware problem.** The instinct to stay on M20 is correct. The fix is
to reduce how many documents each query examines, not to raise the ceiling.

The metric to hold onto throughout: **documents examined per document returned.** A
listing endpoint that returns 20 rows should examine on the order of 20 rows. Where
that ratio runs to thousands, the tier is not the issue.

---

## 2. Finding 1 — the paginated `$lookup` (highest impact)

### What the current pipeline does

```js
[ { $lookup:  { from: "customers", localField: "customerId", … } },
  { $unwind:  "$customer" },
  { $match:   { "customer.region": "EU", "customer.tier": "gold" } },   // ← after the join
  { $sort:    { createdAt: -1 } },
  { $skip:    10000 },
  { $limit:   20 } ]
```

Two independent defects compound:

**a) The filter runs after the join.** Every order in the collection is joined to
`customers` and unwound before the predicate is applied. The server materialises
millions of documents to return twenty.

**b) `$skip` does not skip.** It generates every preceding row and discards it. Page 1
and page 500 are not comparable operations — reaching page 500 means producing and
throwing away 10,000 fully-joined documents.

**No index can fix this as written.** You cannot index a field that does not exist on the
collection being scanned; `customer.region` only exists after the join.

### Options

**Option B — reverse the join.** Resolve the customer predicate first against an indexed
`customers` query, then drive the orders query from that id set. No migration required,
so this is deployable immediately. It degrades as the matching customer set grows,
because a large `$in` becomes its own cost.

**Option C — denormalise, index, and paginate by keyset.** *Recommended.*

Copy the two filter fields onto the order at write time, then one compound index in
ESR order serves the entire query:

```
{ status: 1, customerRegion: 1, customerTier: 1, createdAt: -1 }
   \__________ Equality __________/  \___ Sort / Range ___/
```

Replace `$skip` with a range cursor on `createdAt`. Page depth stops mattering: the
index is seeked directly to the cursor position, and exactly `limit` documents are read.

**The trade-off, stated honestly:** denormalisation creates a write-side consistency
obligation. If a customer changes region or tier, their historical orders no longer
reflect the current value. For an order record this is often *desirable* — an order
should arguably capture the customer's attributes at the time of purchase — but it is a
product decision, not a technical one, and it should be made deliberately rather than
inherited from an index design.

---

## 3. Finding 2 — the unindexed search

An unanchored `$regex` cannot use an index. MongoDB reads every document in the
collection and applies the pattern (COLLSCAN). On a 30 GB dataset with a 4 GB cache,
that is a guaranteed full-collection disk sweep on every keystroke of a search box.

Three remedies, in increasing cost:

1. **Anchor the pattern** (`/^term/`) against an indexed field — becomes a b-tree range
   scan (IXSCAN). Free. Sufficient when users search by prefix, e.g. an order number.
2. **A text index** — handles word-level matching, still free, but has real limitations
   around partial words and language.
3. **Atlas Search** — genuinely better for substring and fuzzy matching, and the right
   answer if the search box must behave like a search engine. It is a **separately
   billed service**, so it should be justified by a measured requirement rather than
   adopted by default.

**Recommendation: measure the actual query shapes before spending.** If most searches
are prefix lookups on identifiers, option 1 solves it at zero cost.

---

## 4. Supporting recommendations

**Bound every query.** `maxTimeMS` on the aggregation pipelines means a pathological
query is killed rather than allowed to pin the primary. This is the cheapest single
protection against the election problem.

**Size the connection pool deliberately.** Spring Data's default pool multiplied by the
number of service instances can exceed what an M20 will accept. Set
`maxPoolSize`, `socketTimeoutMS` and `serverSelectionTimeoutMS` explicitly.

**Working set and retention.** Once the query fixes land, re-measure cache pressure. If
the hot set still exceeds 4 GB, archival of cold collections is the next lever — and
considerably cheaper than a tier upgrade.

**Alerting.** CPU, slow-op count, connection count, and primary elections, with a short
runbook describing the first three things to check.

---

## 5. Suggested sequencing

| Phase | Work | Risk |
|---|---|---|
| 1 | Profile, `explain()` the top operations, produce this analysis | None — read-only |
| 2 | Anchored search index; `maxTimeMS`; pool tuning | Low — additive |
| 3 | Reverse-join fix (Option B) on the listing endpoint | Low — no migration |
| 4 | Denormalisation migration + ESR index + keyset pagination (Option C) | Medium — backfill and an API change |
| 5 | Retention policy, alerting, runbook | Low |

Phases 1–3 are deployable without a data migration and should recover most of the
latency. Phase 4 is what makes deep pagination permanently cheap.
