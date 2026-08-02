# Architecture & Design — MongoDB Atlas listing and search performance

**Problem statement source:** Upwork job `~022067836771266137767`
**Author:** Yuvaraj Aravindan · AI Agentic Enterprises
**Status:** proposal-stage design, validated against an independent reproduction

---

## 1. Problem statement (as given)

> Production SaaS backend on MongoDB Atlas — replica set, **M20 tier, ~30 GB dataset**.
> Backend is **Java / Spring Boot microservices using Spring Data MongoDB**. Under load:
> slow queries and heavy aggregations, occasional CPU spikes and brief replica-set
> elections, and some list/search endpoints that take **minutes instead of milliseconds**.
> **Requirement: stay on M20.** Optimise queries, indexes and configuration to fit within
> current resources. Tier change is a last resort.

Two specific offenders named:

1. A paginated listing endpoint performing `$lookup` over a millions-of-documents
   collection, then filtering **after** the join.
2. An unindexed regex/text search that scans the collection.

---

## 2. Root cause

An M20 provides **4 GB of RAM**. The dataset is **~30 GB**. The working set is therefore
roughly **7× larger than cache** and cannot be resident.

Every symptom follows from that one fact:

```
 query examines N documents
        │
        ├─ N ≫ cache  ──▶ page faults ──▶ disk reads ──▶ latency + CPU
        │                                      │
        │                                      └─▶ primary saturated
        │                                              │
        └─ in-memory sort of joined docs ─────────────┴─▶ missed heartbeats
                                                           │
                                                           └─▶ replica-set election
```

**The tier is not the bottleneck; the query shape is.** The governing metric is
**documents examined per document returned**. A listing endpoint returning 20 rows should
examine on the order of 20. Where that ratio reaches thousands, no realistic amount of RAM
will save it — it will simply fail more slowly at a higher price.

The client's instinct to stay on M20 is correct and this design demonstrates it is
achievable.

---

## 3. Current-state analysis

### 3.1 The listing pipeline

```js
[ { $lookup:  { from: "customers", localField: "customerId",
                foreignField: "customerId", as: "customer" } },
  { $unwind:  "$customer" },
  { $match:   { status: "COMPLETED",
                "customer.region": "EU", "customer.tier": "gold" } },   // ← post-join
  { $sort:    { createdAt: -1 } },
  { $skip:    10000 },
  { $limit:   20 } ]
```

Two independent defects, which compound:

**Defect 1 — the predicate runs after the join.** Every order is joined and unwound before
filtering. Millions of documents are materialised to return twenty.

**Defect 2 — `$skip` does not skip.** It generates and discards every preceding row. Page 1
and page 500 are not comparable operations.

**No index can repair this as written.** `customer.region` does not exist on `orders`; you
cannot index a field on a collection that does not contain it.

### 3.2 The search

Unanchored `$regex` is unindexable. MongoDB reads every document and applies the pattern
(COLLSCAN). The worst case is not a common term — it is a **rare or absent** one, where the
scan must traverse the entire collection before reporting "no results". That is precisely
what a user does when hunting for a specific order.

---

## 4. Validation — measured, not asserted

Reproduced independently on MongoDB 7 Community. Reproducing 30 GB is unnecessary; what must
be faithful is the **data-to-cache ratio**:

| | Client (M20) | Reproduction |
|---|---|---|
| Data | ~30 GB | 1.49 GB |
| WiredTiger cache | 4 GB | 0.25 GB |
| **Ratio** | **~7:1** | **~6:1** |

Dataset: 4,000,000 orders × 2,000,000 customers. Every query capped at 30 s.

| Strategy | Page 1 | Page 500 | docsExamined | examined/returned |
|---|---|---|---|---|
| **A** — current implementation | **timeout >30 s** | **timeout >30 s** | — | — |
| **B** — reverse the join | 155 ms | **timeout >30 s** | — | — |
| **C** — denormalise + ESR + keyset | **12 ms** | **10 ms** | **20** | **1** |

| Search | Time | docsExamined |
|---|---|---|
| **D1** — unanchored `/term/`, rare term | **6,277 ms** | **4,000,000** |
| **D2** — anchored `/^term/` + index | **2 ms** | **1** |

> Before a `maxTimeMS` ceiling was introduced, strategy A ran for **over eleven minutes on
> page 1** without returning. The reported symptom is reproducible.

Three conclusions:

- **C is flat with page depth** — 12 ms at page 1, 10 ms at page 500. Depth stops mattering.
- **C achieves a 1:1 examined-to-returned ratio** — the theoretical optimum.
- **B partially fixes it** — works shallow, fails deep. This proves `$skip` is a *separate*
  defect from the join, and that fixing only the join is insufficient.

---

## 5. Target design

### 5.1 Data model change

Copy the two attributes the listing filters on onto the order at write time:

```
orders {
  …
  customerId:      <int>
  customerRegion:  <string>   ← denormalised
  customerTier:    <string>   ← denormalised
  createdAt:       <date>
}
```

### 5.2 Index

One compound index in **ESR** order — Equality, Sort, Range:

```
{ status: 1, customerRegion: 1, customerTier: 1, createdAt: -1 }
   \____________ Equality ____________/  \___ Sort / Range ___/
```

Serves the filter, the sort and the pagination cursor in a single traversal. `createdAt`
is last because it is both the sort key and the range predicate.

### 5.3 Pagination

Replace offset pagination with a **keyset (range) cursor**:

```js
// page 1
{ status:"COMPLETED", customerRegion:"EU", customerTier:"gold" }
// page n+1 — carry the last row's sort key
{ …, createdAt: { $lt: <createdAt of last row on page n> } }
```

The index is seeked directly to the cursor; exactly `limit` documents are read regardless
of depth.

**API consequence:** the endpoint returns a `nextCursor` instead of accepting a page number.
Clients paginate forward/backward rather than jumping to arbitrary pages. If jump-to-page is
a hard product requirement, that is a genuine constraint to discuss — but it is worth
confirming it is actually used before preserving it at this cost.

### 5.4 Search

Three options, increasing in cost:

| Option | Cost | Suitable when |
|---|---|---|
| Anchored `/^term/` + index | Free | Prefix lookups on identifiers |
| Text index | Free | Word-level matching |
| Atlas Search | **Billed separately** | True substring/fuzzy search |

**Recommendation: measure the real query shapes first.** If most searches are prefix lookups
on order numbers, option 1 solves it at zero cost. Atlas Search should be justified by a
measured requirement, not adopted by default.

---

## 6. Trade-offs, stated plainly

**Denormalisation creates a write-side consistency obligation.** If a customer changes
region or tier, historical orders no longer reflect the current value.

For an order record this is frequently *desirable* — an order arguably should capture the
customer's attributes at the time of purchase. But that is a **product decision, not a
technical one**, and it should be made deliberately rather than inherited as a side effect
of an index design. If current-value semantics are required, the options are a backfill job
on customer update, or a change-stream listener.

**Keyset pagination removes jump-to-page.** See 5.3.

**The migration is a bulk write.** On a live M20 it must be batched and throttled, or it
will itself cause the CPU spike it is meant to prevent.

---

## 7. Supporting changes

| Change | Purpose |
|---|---|
| `maxTimeMS` on all aggregations | A pathological query is killed rather than pinning the primary. Cheapest single protection against the election problem. |
| Connection pool sizing | Spring Data's default pool × instance count can exceed what an M20 accepts. Set `maxPoolSize`, `socketTimeoutMS`, `serverSelectionTimeoutMS` explicitly. |
| Working-set re-measurement | After the query fixes, re-check cache pressure before considering anything else. |
| Retention / archival | If the hot set still exceeds 4 GB, archiving cold data is far cheaper than a tier upgrade. |
| Alerting + runbook | CPU, slow-op count, connections, primary elections — with a one-page first-response guide. |

---

## 8. Delivery plan

| Phase | Work | Risk | Reversible |
|---|---|---|---|
| **1** | Profile; `explain()` the top operations; findings report | None — read-only | n/a |
| **2** | Anchored search index; `maxTimeMS`; pool tuning | Low — additive | Yes |
| **3** | Reverse-join fix on the listing endpoint | Low — no migration | Yes |
| **4** | Denormalisation migration + ESR index + keyset pagination | Medium — backfill + API change | Index yes; API change needs a version |
| **5** | Retention policy, alerting, runbook | Low | Yes |

Phases 1–3 require **no data migration** and should recover most of the latency. Phase 4 is
what makes deep pagination permanently cheap.

### Rollback

- Phases 2–3: revert the deployment; indexes can be dropped online.
- Phase 4: the denormalised fields are additive — leaving them in place is harmless. Roll
  back the API layer to strategy B and the system remains functional.

---

## 9. The reproduction harness

Built to validate the above before any access to production. It doubles as the acceptance
test: the same measurements can be re-run against the real cluster.

```
docker-compose.yml   MongoDB 7, cache size parameterised
bench/               seed · denormalisation migration · benchmark
spring/              the same four strategies in Spring Data MongoDB
demo/                browser UI — drive the strategies live
docs/                findings report · this document
```

The Spring Data implementation matters: the client's backend is Spring Data MongoDB, so the
fix must land as `Aggregation` code that drops into their codebase, not as shell snippets.

**Repository:** https://github.com/YuvarajAravindan-AI-Agent/mongodb-lookup-pagination-benchmark

---

## 10. Scope note

The posted budget is **$100 fixed** against a scope covering profiling, index redesign,
aggregation restructuring, connection-pool tuning, working-set analysis, retention policy,
alerting and a runbook — realistically **20+ hours**.

This design proposes **Phase 1 as the fixed-price engagement**: profiler analysis,
`explain()` evidence on the top five operations, and a prioritised findings report with the
specific index and pipeline changes. Phases 2–5 quoted separately once the findings are
agreed.

That way the budget buys real, complete work rather than a rushed pass at everything.
