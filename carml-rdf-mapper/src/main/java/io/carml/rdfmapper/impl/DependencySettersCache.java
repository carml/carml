package io.carml.rdfmapper.impl;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

class DependencySettersCache {

    private Map<Class<?>, List<Consumer<Object>>> cache = new LinkedHashMap<>();

    Optional<List<Consumer<Object>>> get(Class<?> cls) {
        return cache.containsKey(cls) ? Optional.of(cache.get(cls)) : Optional.empty();
    }

    void put(Class<?> cls, List<Consumer<Object>> dependencySetters) {
        cache.put(cls, dependencySetters);
    }
}
