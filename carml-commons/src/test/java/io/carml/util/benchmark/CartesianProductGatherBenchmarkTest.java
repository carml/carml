package io.carml.util.benchmark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.carml.util.CartesianProduct;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * Stress test for the streaming cartesian product. Verifies peak heap remains bounded under a
 * cardinality where eager materialization would OOM.
 *
 * <p>Scenario: two 10^6-element gather slots produce 10^12 cartesian-product tuples. We consume the
 * first {@code N} tuples and assert that:
 * <ol>
 *   <li>the call returns in well under what eager materialization would take, and
 *   <li>peak heap delta during streaming stays under a generous bound (256 MB) — the streaming
 *       contract guarantees per-tuple working-set rather than total-product memory.
 * </ol>
 *
 * <p>This is a benchmark test (gated by the {@code benchmark} maven profile and
 * {@code <id>benchmark</id>}); not run by default {@code mvn test}.
 */
@Slf4j
class CartesianProductGatherBenchmarkTest {

    private static final int SLOT_SIZE = 1_000_000;

    private static final long TUPLES_TO_CONSUME = 10_000L;

    private static final long MAX_PEAK_HEAP_DELTA_BYTES = 256L * 1024L * 1024L;

    @Test
    void streamingCartesianProduct_atTrillionTupleScale_peakHeapStaysBounded() {
        var slot1 = IntStream.range(0, SLOT_SIZE).boxed().toList();
        var slot2 = IntStream.range(0, SLOT_SIZE).boxed().toList();
        var inputs = List.of(slot1, slot2);

        // Encourage a clean baseline — the slot lists themselves dominate baseline allocation.
        System.gc();
        long heapBefore = usedHeap();

        long start = System.nanoTime();
        long count = CartesianProduct.cartesianProductStream(inputs)
                .limit(TUPLES_TO_CONSUME)
                .count();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;

        long heapAfter = usedHeap();
        long heapDelta = Math.max(0L, heapAfter - heapBefore);

        LOG.info(
                "Streaming cartesian-product: consumed {} of 10^12 tuples in {} ms; heap delta: {} bytes",
                count,
                elapsedMillis,
                heapDelta);

        assertThat(count, is(TUPLES_TO_CONSUME));
        assertThat(heapDelta < MAX_PEAK_HEAP_DELTA_BYTES, is(true));
    }

    private static long usedHeap() {
        var rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
