package io.carml.logicalview.sql;

import io.carml.logicalview.ViewIteration;
import io.carml.model.ReferenceFormulation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;

/**
 * A {@link ViewIteration} backed by a reactive SQL query result row.
 *
 * <p>Reactive SQL result rows do not carry per-field reference formulations, so
 * {@link #getFieldReferenceFormulation(String)} always returns empty. Natural datatypes are inferred
 * from Java types returned by the Vert.x SQL client via the configured {@link SqlClientProvider}.
 */
class ReactiveSqlViewIteration implements ViewIteration {

    private final int index;

    private final Map<String, Object> values;

    private final Map<String, IRI> naturalDatatypes;

    private final Set<String> referenceableKeys;

    /**
     * Creates a new reactive SQL view iteration.
     *
     * @param index the zero-based iteration index
     * @param values the column name to value map; may contain {@code null} values (e.g. from LEFT
     *     JOINs)
     * @param naturalDatatypes the field name to XSD datatype IRI map for type-inferred fields
     * @param referenceableKeys the set of valid referenceable keys for the view, or {@code null} if
     *     not tracked
     */
    ReactiveSqlViewIteration(
            int index, Map<String, Object> values, Map<String, IRI> naturalDatatypes, Set<String> referenceableKeys) {
        this.index = index;
        // Use Collections.unmodifiableMap to support null values (left join no-match fields)
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        this.naturalDatatypes = Map.copyOf(naturalDatatypes);
        this.referenceableKeys = referenceableKeys;
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
        return Optional.empty();
    }

    @Override
    public Optional<IRI> getNaturalDatatype(String key) {
        return Optional.ofNullable(naturalDatatypes.get(key));
    }

    @Override
    public Optional<Set<String>> getReferenceableKeys() {
        return Optional.ofNullable(referenceableKeys);
    }
}
