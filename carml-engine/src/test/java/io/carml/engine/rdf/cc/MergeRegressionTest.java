package io.carml.engine.rdf.cc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import io.carml.engine.MergeableMappingResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Regression test for the streaming merge path. Guards Defect-B of Task 6.68: the previous merge
 * implementation used {@code RDFContainers.extract} on the accumulator's RDF model on every merge,
 * giving O(N²) cost across N pieces. The streaming refactor replaces that with an in-place
 * mutable accumulator: {@code merge} appends to the accumulator's element list in
 * O(piece-size), so total cost across N pieces of size k is O(N·k).
 *
 * <p>The test reduces 10,000 single-element pieces and asserts:
 * <ul>
 *   <li>completion in well under 10 seconds (well below the 3-hour KGCW timeout that the original
 *   implementation could blow at production scale);
 *   <li>correct emitted statement count (1 type triple + 10,000 member triples + 1 linking triple
 *   per linking subject/predicate pair).
 * </ul>
 *
 * <p>10,000 cycles × 1 element each is the dominant shape that surfaces the Defect-B asymptote;
 * the documented 100,000-element shape from the task spec is a single-piece scenario covered by
 * {@link RdfCollectionsAndContainersStreamingTest}.
 */
class MergeRegressionTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    void merge_at10kCycles_completesInUnder10Seconds() {
        var container = VF.createBNode("container");
        var subject = VF.createIRI("http://example.com/e/a");
        var predicate = VF.createIRI("http://example.com/ns#with");

        // Build 10,000 single-element pieces (mimics the per-iteration shape of RmlMapper's
        // mergeMergeables flow).
        int pieceCount = 10_000;
        var pieces = new ArrayList<MergeableMappingResult<Value, Statement>>(pieceCount);
        for (int i = 0; i < pieceCount; i++) {
            pieces.add(MergeableRdfContainer.<Value>builder()
                    .type(RDF.BAG)
                    .container(container)
                    .elements(new ArrayList<>(List.of(VF.createLiteral("v" + i))))
                    .linkingSubjects(Set.of(subject))
                    .linkingPredicates(Set.of(predicate))
                    .build());
        }

        long start = System.nanoTime();
        var merged = pieces.stream().reduce(MergeableMappingResult::merge).orElseThrow();
        long mergeMs = (System.nanoTime() - start) / 1_000_000;

        // Merge cost should be linear: a 10K-cycle reduce should complete in well under 10 seconds.
        // The previous O(N²) implementation would take orders of magnitude longer on this input.
        assertThat("Merge should complete in <10s, took " + mergeMs + "ms", mergeMs, is(lessThan(10_000L)));

        // Verify result correctness: emit and count statements.
        long emittedCount = Flux.from(merged.getResults()).count().block();
        // 1 type triple + 10,000 member triples + 1 linking triple = 10,002
        assertThat(emittedCount, is(equalTo((long) (1 + pieceCount + 1))));
    }

    @Test
    void merge_twoPointLinearityCheck_ratioBelowQuadraticThreshold() {
        // Two-point linearity check: confirms total merge cost scales linearly (or near-linearly)
        // with cycle count, not quadratically. Compares 10K cycles vs 1K cycles. Under O(N) the
        // ratio should be ~10×; under the previous O(N²) implementation it would have been ~100×.
        // The 30× ceiling tolerates GC noise, hardware variance, and JIT warmup effects while
        // still catching a regression to anything substantially super-linear.
        var container = VF.createBNode("container");

        // Warmup: run a small reduce once to drive JIT compilation and reduce noise on the
        // subsequent timed runs. Without this, the first measured run is dominated by JIT.
        var warmupPieces = buildSingleElementPieces(container, 100);
        warmupPieces.stream().reduce(MergeableMappingResult::merge).orElseThrow();

        long ms1k = timeReduce(buildSingleElementPieces(container, 1_000));
        long ms10k = timeReduce(buildSingleElementPieces(container, 10_000));

        // With ms1k floor of 1ms (avoid divide-by-zero on very fast hardware), assert the ratio
        // stays below 30× — well below the ~100× ratio that O(N²) would produce.
        long ratio = ms10k / Math.max(1, ms1k);
        assertThat(
                "Ratio 10K/1K=%d (1k=%dms, 10k=%dms) suggests super-linear merge cost".formatted(ratio, ms1k, ms10k),
                ratio,
                is(lessThan(30L)));
    }

    private static List<MergeableMappingResult<Value, Statement>> buildSingleElementPieces(BNode container, int count) {
        var pieces = new ArrayList<MergeableMappingResult<Value, Statement>>(count);
        for (int i = 0; i < count; i++) {
            pieces.add(MergeableRdfContainer.<Value>builder()
                    .type(RDF.BAG)
                    .container(container)
                    .elements(new ArrayList<>(List.of(VF.createLiteral("v" + i))))
                    .build());
        }
        return pieces;
    }

    private static long timeReduce(List<MergeableMappingResult<Value, Statement>> pieces) {
        long start = System.nanoTime();
        pieces.stream().reduce(MergeableMappingResult::merge).orElseThrow();
        return (System.nanoTime() - start) / 1_000_000;
    }

    @Test
    void merge_atManyCyclesWithLargePieces_isLinearInTotalElementCount() {
        // 1000 pieces × 100 elements each = 100,000 total. Same total scale as 100K cycle × 1
        // element each, but with larger piece size to confirm cost scales with total elements not
        // with cycle count alone.
        var container = VF.createBNode("container");

        int pieceCount = 1_000;
        int pieceSize = 100;
        var pieces = new ArrayList<MergeableMappingResult<Value, Statement>>(pieceCount);

        for (int p = 0; p < pieceCount; p++) {
            var values = new ArrayList<Value>(pieceSize);
            for (int i = 0; i < pieceSize; i++) {
                values.add(VF.createLiteral("p" + p + "_v" + i));
            }
            pieces.add(MergeableRdfContainer.<Value>builder()
                    .type(RDF.BAG)
                    .container(container)
                    .elements(values)
                    .build());
        }

        long start = System.nanoTime();
        var merged = pieces.stream().reduce(MergeableMappingResult::merge).orElseThrow();
        long mergeMs = (System.nanoTime() - start) / 1_000_000;

        // Should still be well under 10 seconds.
        assertThat(
                "1000-cycle × 100-element merge should complete in <10s, took " + mergeMs + "ms",
                mergeMs,
                is(lessThan(10_000L)));

        long emittedCount = Flux.from(merged.getResults()).count().block();
        // 1 type triple + 100,000 members
        assertThat(emittedCount, is(equalTo((long) (1 + (pieceCount * pieceSize)))));
    }

    @Test
    void merge_listAt10kCycles_completesInUnder10Seconds() {
        // Same shape as the container test, applied to MergeableRdfList.
        var head = VF.createBNode("head");

        int pieceCount = 10_000;
        var pieces = new ArrayList<MergeableMappingResult<Value, Statement>>(pieceCount);
        for (int i = 0; i < pieceCount; i++) {
            pieces.add(MergeableRdfList.<Value>builder()
                    .head(head)
                    .elements(new ArrayList<>(List.of(VF.createLiteral("v" + i))))
                    .build());
        }

        long start = System.nanoTime();
        var merged = pieces.stream().reduce(MergeableMappingResult::merge).orElseThrow();
        long mergeMs = (System.nanoTime() - start) / 1_000_000;

        assertThat("Merge should complete in <10s, took " + mergeMs + "ms", mergeMs, is(lessThan(10_000L)));

        long emittedCount = Flux.from(merged.getResults()).count().block();
        // For each of 10,000 elements: rdf:first + rdf:rest = 20,000 statements
        assertThat(emittedCount, is(equalTo((long) (pieceCount * 2))));
    }
}
