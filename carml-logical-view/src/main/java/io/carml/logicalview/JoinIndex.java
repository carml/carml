package io.carml.logicalview;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * An index structure for efficiently performing joins between {@link ViewIteration} streams. Stores
 * key-value associations and supports eviction for bounded-memory operation.
 *
 * <p>Implementations follow a strict two-phase lifecycle per join: a synchronous build phase (all
 * {@link #put(Object, Object)} calls) followed by a probe phase (all {@link #get(Object)} calls).
 * Implementations may exploit this contract to defer expensive seal/index work until the first
 * probe. Calling {@link #close()} releases any resources held by the index (for disk-backed
 * implementations, this deletes any temporary files).
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface JoinIndex<K, V> extends AutoCloseable {

    /**
     * Associates the given value with the given key. Multiple values may be associated with the same
     * key.
     *
     * @param key the key
     * @param value the value
     */
    void put(K key, V value);

    /**
     * Returns all values associated with the given key.
     *
     * @param key the key
     * @return a list of values, or an empty list if the key is not present
     */
    List<V> get(K key);

    /**
     * Evicts entries matching the given predicate. This supports bounded-memory operation by
     * allowing removal of stale or expired entries.
     *
     * @param predicate the predicate to match entries for eviction
     */
    void evict(Predicate<Map.Entry<K, V>> predicate);

    /**
     * Returns the total number of entries in the index.
     *
     * @return the number of entries
     */
    long size();

    /**
     * Releases any resources held by the index. For disk-backed implementations this deletes any
     * temporary files. The default implementation is a no-op for in-memory implementations.
     *
     * <p>Implementations must be idempotent — calling {@code close()} more than once must not throw.
     */
    @Override
    default void close() {
        // No-op for in-memory implementations.
    }
}
