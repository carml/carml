package io.carml.logicalview;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

class DefaultViewIteration implements ViewIteration {

    private final int index;

    private final Map<String, Object> values;

    DefaultViewIteration(int index, Map<String, Object> values) {
        this.index = index;
        this.values = Map.copyOf(values);
    }

    @Override
    public Optional<Object> getValue(String key) {
        return Optional.ofNullable(values.get(key));
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public Set<String> getKeys() {
        return values.keySet();
    }
}
