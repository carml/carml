package io.carml.logicalview.duckdb;

import io.carml.logicalview.ViewIteration;
import io.carml.model.ReferenceFormulation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;

/**
 * A {@link ViewIteration} backed by a DuckDB query result row.
 *
 * <p>DuckDB result rows do not carry per-field reference formulations, so
 * {@link #getFieldReferenceFormulation(String)} always returns empty. Natural datatypes are inferred
 * depending on the source type.
 */
class DuckDbViewIteration implements ViewIteration {

    private final int index;

    private final Map<String, Object> values;

    private final Map<String, IRI> naturalDatatypes;

    /**
     * Creates a new DuckDB view iteration.
     *
     * @param index the zero-based iteration index
     * @param values the column name to value map; may contain {@code null} values (e.g. from LEFT
     *     JOINs)
     * @param naturalDatatypes the field name to XSD datatype IRI map for type-inferred fields
     */
    DuckDbViewIteration(int index, Map<String, Object> values, Map<String, IRI> naturalDatatypes) {
        this.index = index;
        // Use Collections.unmodifiableMap to support null values (left join no-match fields)
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
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
        return Optional.empty();
    }

    @Override
    public Optional<IRI> getNaturalDatatype(String key) {
        return Optional.ofNullable(naturalDatatypes.get(key));
    }
}
