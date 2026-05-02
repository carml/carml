package io.carml.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CartesianProduct {

    /**
     * Returns the cartesian product of the given lists, materialized as a {@code List<List<T>>}.
     *
     * <p>For large input sizes the materialized result holds {@code product(|inputs|)} tuples in
     * heap. Prefer {@link #cartesianProductStream(List)} for large inputs that can be consumed
     * lazily — this method is provided for callers that need random access or eager materialization.
     */
    public static <T> List<List<T>> listCartesianProduct(List<List<T>> lists) {
        return cartesianProductStream(lists).toList();
    }

    /**
     * Streams the cartesian product of the given lists lazily, one tuple at a time.
     *
     * <p>Each emitted tuple is a fresh, unmodifiable {@code List<T>} of size {@code lists.size()}.
     * Total tuple count is {@code product(|inputs|)} — computed eagerly with overflow detection.
     *
     * <p>Example. For {@code lists = [["A","B","C"], ["X","Y"]]} the stream emits:
     *
     * <pre>
     * [A, X], [A, Y], [B, X], [B, Y], [C, X], [C, Y]
     * </pre>
     *
     * The rightmost slot ({@code v2}) varies fastest, so consumers see the same order as a
     * naive nested loop {@code for v1 : ... { for v2 : ... { yield (v1, v2); } }}.
     *
     * <p>How the laziness works (mixed-radix odometer iteration). Tuple count is
     * {@code |v1| × |v2| × ...}, so we conceptually count from {@code 0} to {@code total - 1}
     * and decode each index into a tuple. Each slot has its own "radix" (= its size), and the
     * index's digit at slot {@code i} indexes into {@code lists.get(i)}. This is the same idea
     * as a car odometer — but where each digit position can have a different range. Counting
     * the integer index up by 1 advances the rightmost slot; when it wraps, the next slot
     * advances; and so on. Producing tuple #k requires only {@code k} (a single {@code long}),
     * so the stream needs no per-slot iterator state and supports random access.
     *
     * <p>Edge cases:
     * <ul>
     *   <li>When {@code lists} is empty, yields a single empty tuple.
     *   <li>When any sublist is empty, yields no tuples.
     * </ul>
     *
     * @param lists the input lists whose cartesian product is to be streamed
     * @param <T> the element type
     * @return a lazy {@link Stream} over cartesian-product tuples
     * @throws ArithmeticException if the total tuple count overflows {@code long}
     */
    public static <T> Stream<List<T>> cartesianProductStream(List<List<T>> lists) {
        if (lists.isEmpty()) {
            return Stream.of(List.of());
        }

        // If any sublist is empty, the cartesian product is empty.
        for (var sub : lists) {
            if (sub.isEmpty()) {
                return Stream.empty();
            }
        }

        // Compute the total number of tuples with overflow detection. Multiplication is performed
        // in long with Math.multiplyExact, so the call throws ArithmeticException on overflow.
        long totalSize = 1L;
        for (var sub : lists) {
            totalSize = Math.multiplyExact(totalSize, (long) sub.size());
        }

        // Snapshot inputs into a defensive copy so per-tuple lookups operate on a stable view.
        var snapshots = List.copyOf(lists);
        int dimensions = snapshots.size();

        return LongStream.range(0L, totalSize).mapToObj(index -> tupleAtIndex(snapshots, dimensions, index));
    }

    private static <T> List<T> tupleAtIndex(List<List<T>> snapshots, int dimensions, long index) {
        var combined = new ArrayList<T>(dimensions);
        // Mixed-radix decomposition: rightmost dimension is the fastest-varying so the resulting
        // tuple order matches the original nested-loop construction.
        // Pre-fill with nulls so we can write at known indices without growing the list.
        for (int dim = 0; dim < dimensions; dim++) {
            combined.add(null);
        }
        long remaining = index;
        for (int dim = dimensions - 1; dim >= 0; dim--) {
            var slice = snapshots.get(dim);
            int dimSize = slice.size();
            int position = (int) (remaining % dimSize);
            combined.set(dim, slice.get(position));
            remaining /= dimSize;
        }
        return List.copyOf(combined);
    }
}
