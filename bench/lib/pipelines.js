/**
 * The four pipelines under test.
 *
 * All four answer the same business question:
 *   "Give me page N of completed orders belonging to gold-tier customers in the EU,
 *    newest first, with the customer's name attached."
 *
 * They differ only in HOW, and that difference is the entire job.
 */

/** Page size used everywhere so the comparison is honest. */
export const PAGE_SIZE = 20;

/**
 * A — THE ANTI-PATTERN (what the client has today).
 *
 * Joins EVERY order to the customers collection, unwinds, and only THEN
 * discards ~99% of the rows with a post-join $match. Pagination is $skip,
 * which forces the server to generate and throw away every preceding row.
 *
 * Cost profile: docsExamined grows with collection size, not page size.
 * Deep pages get linearly worse. This is why "page 500 takes minutes".
 */
export function naive(skip = 0) {
  return [
    {
      $lookup: {
        from: "customers",
        localField: "customerId",
        foreignField: "customerId",
        as: "customer",
      },
    },
    { $unwind: "$customer" },
    {
      $match: {
        status: "COMPLETED",
        "customer.region": "EU",
        "customer.tier": "gold",
      },
    },
    { $sort: { createdAt: -1 } },
    { $skip: skip },
    { $limit: PAGE_SIZE },
    { $project: { orderNumber: 1, totalAmount: 1, createdAt: 1, "customer.name": 1 } },
  ];
}

/**
 * B — REVERSE THE JOIN.
 *
 * Resolve the selective customer predicate FIRST against an indexed customers
 * query, then drive the orders query from that id set. No full-collection join.
 *
 * Good when the customer filter is selective. Degrades when the matching
 * customer set is large, because $in on a huge array is its own problem —
 * which is exactly why C exists.
 */
export function reverseJoin(customerIds, skip = 0) {
  return [
    { $match: { status: "COMPLETED", customerId: { $in: customerIds } } },
    { $sort: { createdAt: -1 } },
    { $skip: skip },
    { $limit: PAGE_SIZE },
    {
      $lookup: {
        from: "customers",
        let: { cid: "$customerId" },
        pipeline: [
          { $match: { $expr: { $eq: ["$customerId", "$$cid"] } } },
          { $project: { _id: 0, name: 1 } },
        ],
        as: "customer",
      },
    },
    { $unwind: "$customer" },
    { $project: { orderNumber: 1, totalAmount: 1, createdAt: 1, "customer.name": 1 } },
  ];
}

/**
 * C — DENORMALISE + COMPOUND INDEX + RANGE PAGINATION.  ← the recommendation
 *
 * The filter fields the listing sorts and filters on (region, tier) are copied
 * onto the order at write time. One compound index in ESR order then serves the
 * whole query, and the $lookup is reduced to a covered fetch of 20 names.
 *
 * ESR = Equality, Sort, Range:
 *    { status: 1, customerRegion: 1, customerTier: 1,   <- Equality
 *      createdAt: -1 }                                  <- Sort (and Range cursor)
 *
 * Pagination is a range cursor on the indexed sort key, NOT $skip. Page 1 and
 * page 5000 cost the same. This is the change that turns minutes into
 * milliseconds, and it is the one worth arguing for.
 */
export function denormalised(cursor = null) {
  const match = { status: "COMPLETED", customerRegion: "EU", customerTier: "gold" };
  if (cursor) match.createdAt = { $lt: cursor };
  return [
    { $match: match },
    { $sort: { createdAt: -1 } },
    { $limit: PAGE_SIZE },
    {
      $lookup: {
        from: "customers",
        let: { cid: "$customerId" },
        pipeline: [
          { $match: { $expr: { $eq: ["$customerId", "$$cid"] } } },
          { $project: { _id: 0, name: 1 } },
        ],
        as: "customer",
      },
    },
    { $unwind: "$customer" },
    { $project: { orderNumber: 1, totalAmount: 1, createdAt: 1, "customer.name": 1 } },
  ];
}

/**
 * D — the regex problem, both ways.
 *
 * Unanchored /term/ cannot use an index: every document must be read and the
 * pattern applied. Anchored /^term/ on an indexed field becomes a b-tree range
 * scan. Same feature, two orders of magnitude apart.
 */
export const regex = {
  unanchored: (term) => ({ notes: { $regex: term, $options: "i" } }),
  anchoredIndexed: (term) => ({ searchKey: { $regex: `^${term}` } }),
};
