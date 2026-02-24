package io.carml.logicalview;

import io.carml.model.ReferenceFormulation;
import java.util.Optional;
import java.util.Set;

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
}
