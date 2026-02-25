package io.carml.logicalview;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

class HashMapJoinIndex<K, V> implements JoinIndex<K, V> {

    private final Map<K, List<V>> index = new HashMap<>();

    @Override
    public void put(K key, V value) {
        index.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    @Override
    public List<V> get(K key) {
        var values = index.get(key);
        return values != null ? List.copyOf(values) : List.of();
    }

    @Override
    public void evict(Predicate<Map.Entry<K, V>> predicate) {
        // Batch mode: no eviction needed
    }

    @Override
    public long size() {
        return index.values().stream().mapToLong(List::size).sum();
    }
}
