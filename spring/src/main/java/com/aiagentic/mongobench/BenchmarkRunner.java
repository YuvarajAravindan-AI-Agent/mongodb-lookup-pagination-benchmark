package com.aiagentic.mongobench;

import com.aiagentic.mongobench.pipeline.OrderListingAggregations;
import org.bson.Document;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs each strategy against the seeded dataset and prints the numbers that go
 * into the client report.
 *
 * <p>Reports {@code totalDocsExamined} alongside {@code nReturned}. The ratio
 * between them is the inefficiency — documents the server read for every
 * document the user actually received. On a cache far smaller than the dataset,
 * that ratio is what evicts hot pages, spikes CPU, and stalls the primary long
 * enough to trigger a replica-set election.
 */
@Component
public class BenchmarkRunner implements CommandLineRunner {

    private final MongoTemplate mongo;

    public BenchmarkRunner(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    @Override
    public void run(String... args) {
        System.out.println("""

                Spring Data MongoDB — $lookup pagination benchmark
                =================================================""");

        for (int page : new int[]{1, 500}) {
            long skip = (long) (page - 1) * OrderListingAggregations.PAGE_SIZE;
            System.out.printf("%n--- page %d (skip %d) ---%n", page, skip);

            timed("A  lookup-then-filter + skip", OrderListingAggregations.naive(skip));

            List<Integer> ids = goldEuCustomerIds();
            timed("B  reverse join (" + ids.size() + " ids)",
                    OrderListingAggregations.reverseJoin(ids, skip));

            Instant cursor = cursorForPage(skip);
            timed("C  denormalised + ESR + keyset",
                    OrderListingAggregations.denormalised(cursor));
        }

        System.out.println("\n--- regex ---");
        timedFind("D1 unanchored  (COLLSCAN)",
                OrderListingAggregations.regexUnanchored("refund"));
        timedFind("D2 anchored+ix (IXSCAN)",
                OrderListingAggregations.regexAnchoredIndexed("ORD000500000"));

        System.out.println("\ndone.");
    }

    /** The selective customer slice strategy B depends on. */
    private List<Integer> goldEuCustomerIds() {
        Query q = Query.query(Criteria.where("region").is("EU").and("tier").is("gold"));
        q.fields().include("customerId").exclude("_id");
        List<Integer> ids = new ArrayList<>();
        for (Document d : mongo.find(q, Document.class, "customers")) {
            ids.add(d.getInteger("customerId"));
        }
        return ids;
    }

    /**
     * Keyset pagination has no "skip" — to reach page N you carry the sort key
     * of the previous page's last row. Resolving it here is itself honest cost,
     * and still far cheaper than making the server generate and discard rows.
     */
    private Instant cursorForPage(long skip) {
        if (skip == 0) return null;
        Query q = Query.query(Criteria.where("status").is("COMPLETED")
                        .and("customerRegion").is("EU")
                        .and("customerTier").is("gold"))
                .with(org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "createdAt"))
                .skip(skip - 1)
                .limit(1);
        q.fields().include("createdAt");
        Document d = mongo.findOne(q, Document.class, "orders");
        return d == null ? null : d.getDate("createdAt").toInstant();
    }

    private void timed(String label, Aggregation agg) {
        long t0 = System.nanoTime();
        var results = mongo.aggregate(agg, "orders", Document.class).getMappedResults();
        long ms = (System.nanoTime() - t0) / 1_000_000;

        Document explain = explainAggregate(agg);
        Stats s = Stats.from(explain);
        System.out.printf("  %-34s %7d ms   docsExamined=%,12d  nReturned=%3d  ratio=%s%n",
                label, ms, s.docsExamined, results.size(),
                results.isEmpty() ? "—" : String.format("%,d", s.docsExamined / Math.max(1, results.size())));
    }

    private void timedFind(String label, Criteria criteria) {
        Query q = Query.query(criteria).limit(20);
        long t0 = System.nanoTime();
        var results = mongo.find(q, Document.class, "orders");
        long ms = (System.nanoTime() - t0) / 1_000_000;

        Document explain = mongo.getDb().runCommand(new Document("explain",
                        new Document("find", "orders")
                                .append("filter", q.getQueryObject())
                                .append("limit", 20))
                .append("verbosity", "executionStats"));
        Stats s = Stats.from(explain);
        System.out.printf("  %-34s %7d ms   docsExamined=%,12d  nReturned=%3d%n",
                label, ms, s.docsExamined, results.size());
    }

    private Document explainAggregate(Aggregation agg) {
        Document command = new Document("explain",
                new Document("aggregate", "orders")
                        .append("pipeline", agg.toPipeline(
                                org.springframework.data.mongodb.core.aggregation.Aggregation.DEFAULT_CONTEXT))
                        .append("cursor", new Document()))
                .append("verbosity", "executionStats");
        return mongo.getDb().runCommand(command);
    }

    /** Walks an explain tree accumulating the counters, wherever they nest. */
    private record Stats(long docsExamined, long keysExamined) {
        static Stats from(Document explain) {
            long[] acc = new long[2];
            walk(explain, acc);
            return new Stats(acc[0], acc[1]);
        }

        @SuppressWarnings("unchecked")
        private static void walk(Object node, long[] acc) {
            if (node instanceof Document d) {
                if (d.get("executionStats") instanceof Document es) {
                    acc[0] += num(es.get("totalDocsExamined"));
                    acc[1] += num(es.get("totalKeysExamined"));
                }
                d.values().forEach(v -> walk(v, acc));
            } else if (node instanceof List<?> l) {
                l.forEach(v -> walk(v, acc));
            }
        }

        private static long num(Object o) {
            return o instanceof Number n ? n.longValue() : 0L;
        }
    }
}
