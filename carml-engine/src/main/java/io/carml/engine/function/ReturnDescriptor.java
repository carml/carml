package io.carml.engine.function;

import org.eclipse.rdf4j.model.IRI;

/**
 * Describes a return value of a {@link FunctionDescriptor}.
 *
 * @param predicateIri the predicate IRI from {@code fno:predicate}, may be {@code null} for single-return functions
 * @param type the Java type of the return value
 */
public record ReturnDescriptor(IRI predicateIri, Class<?> type) {

    public ReturnDescriptor {
        if (type == null) {
            throw new IllegalArgumentException("Return type must not be null");
        }
    }

    /**
     * Checks whether the given IRI matches this return descriptor's {@code fno:predicate} IRI.
     */
    public boolean matches(IRI candidate) {
        if (candidate == null) {
            return false;
        }
        return candidate.equals(predicateIri);
    }
}
