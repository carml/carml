package io.carml.functions;

import org.eclipse.rdf4j.model.IRI;

/**
 * Describes a parameter accepted by a {@link FunctionDescriptor}.
 *
 * @param parameterIri the IRI of the {@code fno:Parameter} resource
 * @param type the Java type of the parameter
 * @param required whether this parameter is required
 */
public record ParameterDescriptor(IRI parameterIri, Class<?> type, boolean required) {

    public ParameterDescriptor {
        if (parameterIri == null) {
            throw new IllegalArgumentException("Parameter IRI must not be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("Parameter type must not be null");
        }
    }

    /**
     * Checks whether the given IRI matches this parameter's resource IRI.
     */
    public boolean matches(IRI candidate) {
        if (candidate == null) {
            return false;
        }
        return candidate.equals(parameterIri);
    }
}
