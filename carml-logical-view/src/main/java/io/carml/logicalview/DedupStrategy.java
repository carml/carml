package io.carml.logicalview;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Flux;

/**
 * Strategy for deduplicating {@link ViewIteration} streams based on key fields. Different
 * strategies trade off between exactness, memory usage, and latency.
 */
public interface DedupStrategy {

    /**
     * Deduplicates the given source flux based on the specified key fields.
     *
     * @param source the source flux of view iterations
     * @param keyFields the fields to use as the deduplication key
     * @return a deduplicated flux
     */
    Flux<ViewIteration> deduplicate(Flux<ViewIteration> source, Set<String> keyFields);

    /**
     * Returns an exact deduplication strategy using a thread-safe {@link ConcurrentHashMap}-backed
     * set to track seen composite keys. For each iteration, the key field values are extracted into
     * a list; iterations with a previously seen combination are filtered out. Absent field values
     * (e.g. from left joins) are represented as {@code null} within the key list. Guarantees perfect
     * deduplication but may use significant memory for large datasets.
     *
     * @return an exact dedup strategy
     */
    static DedupStrategy exact() {
        return (source, keyFields) -> {
            Set<List<Object>> seen = ConcurrentHashMap.newKeySet();
            return source.filter(iteration -> {
                var key = keyFields.stream()
                        .sorted()
                        .map(field -> iteration.getValue(field).orElse(null))
                        .toList();
                return seen.add(key);
            });
        };
    }

    /**
     * Returns a windowed deduplication strategy that deduplicates within a time window. This bounds
     * memory usage for streaming scenarios.
     *
     * @param window the time window duration
     * @return a windowed dedup strategy
     * @throws UnsupportedOperationException windowed dedup is deferred to the streaming phase
     */
    static DedupStrategy windowed(Duration window) {
        throw new UnsupportedOperationException("Windowed dedup strategy is not yet implemented");
    }

    /**
     * Returns a probabilistic deduplication strategy using a Bloom filter. This provides
     * constant-memory deduplication with a configurable false-positive rate.
     *
     * @param fpRate the acceptable false-positive rate (e.g. 0.01 for 1%)
     * @param expectedSize the expected number of distinct elements
     * @return a probabilistic dedup strategy
     * @throws UnsupportedOperationException probabilistic dedup is deferred to the streaming phase
     */
    static DedupStrategy probabilistic(double fpRate, long expectedSize) {
        throw new UnsupportedOperationException("Probabilistic dedup strategy is not yet implemented");
    }

    /**
     * Returns a no-op deduplication strategy that passes all iterations through unchanged.
     *
     * @return a pass-through dedup strategy
     */
    static DedupStrategy none() {
        return (source, keyFields) -> source;
    }
}
