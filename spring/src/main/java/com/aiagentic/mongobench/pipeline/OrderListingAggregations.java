package com.aiagentic.mongobench.pipeline;

import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * The same business query expressed four ways, in Spring Data MongoDB.
 *
 * <p>The client's backend is Java / Spring Boot on Spring Data MongoDB, so the
 * fix has to land as {@code Aggregation} code, not as shell one-liners. Every
 * strategy below is drop-in against a {@code MongoTemplate}.
 *
 * <p>Business question in all four cases: <em>page N of COMPLETED orders
 * belonging to gold-tier EU customers, newest first, with the customer name.</em>
 */
public final class OrderListingAggregations {

    public static final int PAGE_SIZE = 20;

    private OrderListingAggregations() {
    }

    /**
     * A — the anti-pattern currently in production.
     *
     * <p>{@code $lookup} runs across the entire orders collection before the
     * predicate is applied, so the server materialises millions of joined
     * documents and then discards ~98% of them. {@code skip()} compounds it:
     * reaching page 500 means generating and throwing away 10,000 rows.
     *
     * <p>Cost scales with collection size and page depth, not with page size.
     * This is why deep pages take minutes.
     */
    public static Aggregation naive(long skip) {
        return Aggregation.newAggregation(
                Aggregation.lookup("customers", "customerId", "customerId", "customer"),
                Aggregation.unwind("customer"),
                Aggregation.match(Criteria.where("status").is("COMPLETED")
                        .and("customer.region").is("EU")
                        .and("customer.tier").is("gold")),
                Aggregation.sort(Sort.Direction.DESC, "createdAt"),
                Aggregation.skip(skip),
                Aggregation.limit(PAGE_SIZE),
                Aggregation.project("orderNumber", "totalAmount", "createdAt")
                        .and("customer.name").as("customerName")
        ).withOptions(explainable());
    }

    /**
     * B — reverse the join.
     *
     * <p>Resolve the selective customer predicate first against an indexed
     * {@code customers} query, then drive orders from that id set. No
     * collection-wide join.
     *
     * <p>Effective when the customer slice is small. Degrades as the {@code $in}
     * array grows — which is the honest limitation, and the reason C exists.
     */
    public static Aggregation reverseJoin(Collection<Integer> customerIds, long skip) {
        return Aggregation.newAggregation(
                Aggregation.match(Criteria.where("status").is("COMPLETED")
                        .and("customerId").in(customerIds)),
                Aggregation.sort(Sort.Direction.DESC, "createdAt"),
                Aggregation.skip(skip),
                Aggregation.limit(PAGE_SIZE),
                customerNameLookup(),
                Aggregation.unwind("customer"),
                Aggregation.project("orderNumber", "totalAmount", "createdAt")
                        .and("customer.name").as("customerName")
        ).withOptions(explainable());
    }

    /**
     * C — denormalised filter fields + ESR compound index + keyset pagination.
     * <strong>The recommendation.</strong>
     *
     * <p>{@code customerRegion} and {@code customerTier} are written onto the
     * order, so one index serves the whole query:
     *
     * <pre>
     *   { status: 1, customerRegion: 1, customerTier: 1, createdAt: -1 }
     *       \________ Equality ________/  \___ Sort/Range ___/
     * </pre>
     *
     * <p>Pagination is a range cursor on the indexed sort key rather than
     * {@code skip()}, so page 1 and page 5,000 cost the same. Pass the
     * {@code createdAt} of the last row of the previous page as {@code cursor}.
     *
     * <p>Trade-off to state plainly to the client: this introduces a write-side
     * consistency obligation. If a customer changes tier, historical orders must
     * either be backfilled or accepted as point-in-time — which for an order
     * record is usually the correct business answer anyway.
     */
    public static Aggregation denormalised(Instant cursor) {
        Criteria criteria = Criteria.where("status").is("COMPLETED")
                .and("customerRegion").is("EU")
                .and("customerTier").is("gold");
        if (cursor != null) {
            criteria = criteria.and("createdAt").lt(cursor);
        }
        MatchOperation match = Aggregation.match(criteria);

        return Aggregation.newAggregation(
                match,
                Aggregation.sort(Sort.Direction.DESC, "createdAt"),
                Aggregation.limit(PAGE_SIZE),
                customerNameLookup(),
                Aggregation.unwind("customer"),
                Aggregation.project("orderNumber", "totalAmount", "createdAt")
                        .and("customer.name").as("customerName")
        ).withOptions(explainable());
    }

    /**
     * Sub-pipeline lookup that projects only the name. Runs against 20 documents
     * because it sits AFTER the limit — the ordering of stages is the point.
     */
    private static org.springframework.data.mongodb.core.aggregation.AggregationOperation customerNameLookup() {
        return context -> new Document("$lookup", new Document()
                .append("from", "customers")
                .append("let", new Document("cid", "$customerId"))
                .append("pipeline", List.of(
                        new Document("$match", new Document("$expr",
                                new Document("$eq", List.of("$customerId", "$$cid")))),
                        new Document("$project", new Document("_id", 0).append("name", 1))
                ))
                .append("as", "customer"));
    }

    /**
     * D — the regex problem.
     *
     * <p>{@code /term/} is unanchored, so no index can serve it: every document
     * is read and the pattern applied (COLLSCAN). {@code /^term/} against an
     * indexed field becomes a b-tree range scan (IXSCAN). Same user-facing
     * feature, two orders of magnitude apart.
     *
     * <p>Atlas Search is the third option and is genuinely better for
     * substring/fuzzy matching — but it is a separate billed service. Measure
     * before recommending it; an anchored index is free.
     */
    public static Criteria regexUnanchored(String term) {
        return Criteria.where("notes").regex(term, "i");
    }

    public static Criteria regexAnchoredIndexed(String term) {
        return Criteria.where("searchKey").regex("^" + java.util.regex.Pattern.quote(term));
    }

    /**
     * allowDiskUse is on so strategy A can actually complete — without it the
     * in-memory sort on a multi-million-document join blows the 100 MB limit and
     * throws. That failure is itself worth showing the client: their current
     * pipeline is one dataset-growth step away from erroring outright.
     */
    private static AggregationOptions explainable() {
        return AggregationOptions.builder().allowDiskUse(true).build();
    }
}
