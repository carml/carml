package io.carml.engine.function;

import org.eclipse.rdf4j.model.IRI;

/**
 * Describes a parameter accepted by a {@link FunctionDescriptor}.
 *
 * @param predicateIri the predicate IRI from {@code fno:predicate} on the parameter resource
 * @param type the Java type of the parameter
 * @param required whether this parameter is required
 */
public record ParameterDescriptor(IRI predicateIri, Class<?> type, boolean required) {

    public ParameterDescriptor {
        if (predicateIri == null) {
            throw new IllegalArgumentException("Parameter IRI must not be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("Parameter type must not be null");
        }
    }

    /**
     * Checks whether the given IRI matches this parameter's {@code fno:predicate} IRI.
     */
    public boolean matches(IRI candidate) {
        if (candidate == null) {
            return false;
        }
        return candidate.equals(predicateIri);
    }
}
