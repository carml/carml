package io.carml.logicalview;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * An index structure for efficiently performing joins between {@link ViewIteration} streams. Stores
 * key-value associations and supports eviction for bounded-memory operation.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface JoinIndex<K, V> {

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
}
