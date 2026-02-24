package io.carml.logicalview;

import io.carml.model.ReferenceFormulation;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class DefaultViewIteration implements ViewIteration {

    private final int index;

    private final Map<String, Object> values;

    private final Map<String, ReferenceFormulation> referenceFormulations;

    DefaultViewIteration(
            int index, Map<String, Object> values, Map<String, ReferenceFormulation> referenceFormulations) {
        this.index = index;
        this.values = Map.copyOf(values);
        this.referenceFormulations = Map.copyOf(referenceFormulations);
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

    @Override
    public Optional<ReferenceFormulation> getFieldReferenceFormulation(String key) {
        return Optional.ofNullable(referenceFormulations.get(key));
    }
}
