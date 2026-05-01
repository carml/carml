package io.carml.functions;

import org.eclipse.rdf4j.model.IRI;

/**
 * Describes a return value of a {@link FunctionDescriptor}.
 *
 * @param outputIri the IRI of the {@code fno:Output} resource, may be {@code null} for
 *     single-return functions with no declared output
 * @param predicateIri the {@code fno:predicate} IRI of the output, may be {@code null}; when
 *     present, {@link #matches(IRI)} also returns true for this IRI
 * @param type the Java type of the return value
 */
public record ReturnDescriptor(IRI outputIri, IRI predicateIri, Class<?> type) {

    public ReturnDescriptor {
        if (type == null) {
            throw new IllegalArgumentException("Return type must not be null");
        }
    }

    public ReturnDescriptor(IRI outputIri, Class<?> type) {
        this(outputIri, null, type);
    }

    /**
     * Checks whether the given IRI matches this return descriptor's output resource IRI or its
     * {@code fno:predicate} IRI.
     */
    public boolean matches(IRI candidate) {
        if (candidate == null) {
            return false;
        }
        return candidate.equals(outputIri) || candidate.equals(predicateIri);
    }
}
