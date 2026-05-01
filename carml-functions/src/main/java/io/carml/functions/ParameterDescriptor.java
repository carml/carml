package io.carml.functions;

import org.eclipse.rdf4j.model.IRI;

/**
 * Describes a parameter accepted by a {@link FunctionDescriptor}.
 *
 * @param parameterIri the canonical IRI of the {@code fno:Parameter} resource (used as the key in
 *     {@link FunctionDescriptor#execute(java.util.Map)})
 * @param predicateIri the {@code fno:predicate} IRI of the parameter, may be {@code null}; when
 *     present, {@link #matches(IRI)} also returns true for this IRI
 * @param type the Java type of the parameter
 * @param required whether this parameter is required
 */
public record ParameterDescriptor(IRI parameterIri, IRI predicateIri, Class<?> type, boolean required) {

    public ParameterDescriptor {
        if (parameterIri == null) {
            throw new IllegalArgumentException("Parameter IRI must not be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("Parameter type must not be null");
        }
    }

    public ParameterDescriptor(IRI parameterIri, Class<?> type, boolean required) {
        this(parameterIri, null, type, required);
    }

    /**
     * Checks whether the given IRI matches this parameter's resource IRI or its
     * {@code fno:predicate} IRI.
     */
    public boolean matches(IRI candidate) {
        if (candidate == null) {
            return false;
        }
        return candidate.equals(parameterIri) || candidate.equals(predicateIri);
    }
}
