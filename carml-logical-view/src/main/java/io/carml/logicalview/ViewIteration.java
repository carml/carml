package io.carml.logicalview;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ReferenceFormulation;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;

/**
 * Represents a single iteration (row) produced by evaluating a {@link io.carml.model.LogicalView}.
 * Each iteration exposes key-value pairs corresponding to the view's projected fields.
 */
public interface ViewIteration {

    /**
     * Returns the value for the given field key, or empty if the field is absent or null.
     *
     * @param key the field key
     * @return an {@link Optional} containing the value, or empty
     */
    Optional<Object> getValue(String key);

    /**
     * Returns the zero-based index of this iteration within the source evaluation.
     *
     * @return the iteration index
     */
    int getIndex();

    /**
     * Returns the set of field keys available in this iteration.
     *
     * @return an unmodifiable set of field keys
     */
    Set<String> getKeys();

    /**
     * Returns the reference formulation under which the given field's value was evaluated, or empty
     * if no reference formulation is tracked for that field.
     *
     * @param key the field key
     * @return an {@link Optional} containing the reference formulation, or empty
     */
    Optional<ReferenceFormulation> getFieldReferenceFormulation(String key);

    /**
     * Returns the natural datatype IRI for the given field key, or empty if no natural datatype is
     * known. Index keys always have natural datatype {@code xsd:integer}.
     *
     * @param key the field key
     * @return an {@link Optional} containing the natural datatype IRI, or empty
     */
    Optional<IRI> getNaturalDatatype(String key);

    /**
     * Returns the source-level expression evaluation for this iteration's underlying source record,
     * if available. This allows expressions that are NOT view fields (e.g., gather map references)
     * to be evaluated directly from the source data.
     *
     * @return the source expression evaluation, or empty if not available
     */
    default Optional<ExpressionEvaluation> getSourceEvaluation() {
        return Optional.empty();
    }

    /**
     * Returns the set of keys that are valid references for this iteration's logical view, if known.
     * This is the schema-level set of declared field names (including index keys and join field keys)
     * that a mapping is allowed to reference. When present, it enables strict validation that
     * distinguishes truly invalid references from expressions that require source-level fallback
     * evaluation (e.g., gather map references).
     *
     * <p>When empty, validation falls back to {@link #getKeys()} (all keys present in this row).
     *
     * @return an {@link Optional} containing the referenceable keys, or empty if not tracked
     */
    default Optional<Set<String>> getReferenceableKeys() {
        return Optional.empty();
    }

    /**
     * Constructs a default {@link ViewIteration} from raw maps. Intended for {@link JoinExecutor}
     * implementations and tests in sibling modules that need to materialize iterations without
     * accessing the package-private default implementation directly.
     *
     * @param index the iteration index
     * @param values the field values keyed by absolute name
     * @param referenceFormulations per-field reference formulations
     * @param naturalDatatypes per-field natural datatypes
     * @return a new {@link ViewIteration}
     */
    static ViewIteration of(
            int index,
            Map<String, Object> values,
            Map<String, ReferenceFormulation> referenceFormulations,
            Map<String, IRI> naturalDatatypes) {
        return new DefaultViewIteration(index, values, referenceFormulations, naturalDatatypes);
    }
}
