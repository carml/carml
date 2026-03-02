package io.carml.logicalview;

import io.carml.model.ReferenceFormulation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;

class DefaultViewIteration implements ViewIteration {

    private final int index;

    private final Map<String, Object> values;

    private final Map<String, ReferenceFormulation> referenceFormulations;

    private final Map<String, IRI> naturalDatatypes;

    DefaultViewIteration(
            int index,
            Map<String, Object> values,
            Map<String, ReferenceFormulation> referenceFormulations,
            Map<String, IRI> naturalDatatypes) {
        this.index = index;
        // Use Collections.unmodifiableMap to support null values (left join no-match fields)
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        this.referenceFormulations = Map.copyOf(referenceFormulations);
        this.naturalDatatypes = Map.copyOf(naturalDatatypes);
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

    @Override
    public Optional<IRI> getNaturalDatatype(String key) {
        return Optional.ofNullable(naturalDatatypes.get(key));
    }

    DefaultViewIteration withIndex(int newIndex) {
        var newValues = new LinkedHashMap<>(this.values);
        newValues.put(DefaultLogicalViewEvaluator.INDEX_KEY, newIndex);
        var newNaturalDatatypes = new LinkedHashMap<>(this.naturalDatatypes);
        newNaturalDatatypes.put(DefaultLogicalViewEvaluator.INDEX_KEY, XSD.INTEGER);
        return new DefaultViewIteration(newIndex, newValues, this.referenceFormulations, newNaturalDatatypes);
    }
}
