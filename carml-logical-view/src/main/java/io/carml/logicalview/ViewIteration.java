package io.carml.logicalview;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ReferenceFormulation;
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
}
