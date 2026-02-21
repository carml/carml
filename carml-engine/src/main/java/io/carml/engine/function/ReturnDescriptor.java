package io.carml.engine.function;

import org.eclipse.rdf4j.model.IRI;

/** Describes a return value of a {@link FunctionDescriptor}. The IRI may be {@code null} for single-return functions. */
public record ReturnDescriptor(IRI iri, Class<?> type) {

    public ReturnDescriptor {
        if (type == null) {
            throw new IllegalArgumentException("Return type must not be null");
        }
    }
}
