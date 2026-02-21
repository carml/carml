package io.carml.engine.function;

import org.eclipse.rdf4j.model.IRI;

/** Describes a parameter accepted by a {@link FunctionDescriptor}. */
public record ParameterDescriptor(IRI iri, Class<?> type, boolean required) {

    public ParameterDescriptor {
        if (iri == null) {
            throw new IllegalArgumentException("Parameter IRI must not be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("Parameter type must not be null");
        }
    }
}
