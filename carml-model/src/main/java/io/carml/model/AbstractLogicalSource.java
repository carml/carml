package io.carml.model;

import java.util.Optional;

/**
 * Abstract base for both {@link LogicalSource} and {@code LogicalView}. TriplesMap.getLogicalSource()
 * returns this type to support both.
 */
public interface AbstractLogicalSource extends Resource {

    /**
     * Resolves the effective iterator for this source, applying any spec-mandated default from the
     * reference formulation when no iterator is declared. Returns the raw iterator value as
     * {@link Object} so that custom or future formulations may use non-string iterator
     * representations. For all current built-in formulations the value is a {@link String} —
     * callers in those paths typically use {@link #resolveIteratorAsString()} for convenience.
     *
     * <p>Returns empty when no iterator is declared and the formulation defines no default
     * (e.g. CSV row-based, SQL table). The default for {@link AbstractLogicalSource} is empty;
     * {@link LogicalSource} overrides this with the actual iterator lookup.
     */
    default Optional<Object> resolveIterator() {
        return Optional.empty();
    }

    /**
     * String-typed convenience around {@link #resolveIterator()} for the common case where the
     * iterator is a plain string (every built-in formulation today). Coerces the raw value via
     * {@link Object#toString()}; callers needing the raw object form should use
     * {@link #resolveIterator()} directly.
     */
    default Optional<String> resolveIteratorAsString() {
        return resolveIterator().map(Object::toString);
    }
}
