# Measured results

MongoDB 7 Community · 4,000,000 orders × 2,000,000 customers · ~1.3 GB data
WiredTiger cache **0.25 GB** — preserving the client's ~7:1 data-to-cache ratio on an M20.
Every query capped at 30s.

| Strategy | Page | Median ms | docsExamined | keysExamined | nReturned | examined per returned |
|---|---|---:|---:|---:|---:|---:|
| A  lookup-then-filter + $skip | page 1 | **timeout >30s** | — | — | — | — |
| B  reverse join ($in) | page 1 | 155 | 0 | 0 | 20 | 0 |
| C  denormalised + ESR + keyset | page 1 | 12 | 20 | 20 | 20 | 1 |
| A  lookup-then-filter + $skip | page 500 | **timeout >30s** | — | — | — | — |
| B  reverse join ($in) | page 500 | **timeout >30s** | — | — | — | — |
| C  denormalised + ESR + keyset | page 500 | 10 | 20 | 20 | 20 | 1 |
| D1 unanchored, rare term (COLLSCAN) | search | 6277 | 4,000,000 | 0 | 0 | 0 |
| D2 anchored /^term/ + ix (IXSCAN) | search | 2 | 1 | 2 | 1 | 1 |

**`examined per returned`** is the inefficiency: documents the server read for every
document the user received. On a cache far smaller than the dataset, that ratio is
what evicts hot pages, spikes CPU, and stalls the primary into a replica-set election.

> Before the cap was added, strategy A ran for **over eleven minutes on page 1**
> without returning. That is the client's reported symptom, reproduced.