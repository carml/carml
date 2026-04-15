package io.carml.output;

/**
 * Thrown by an {@link RdfSerializer} when the underlying serialization implementation fails.
 * Wraps implementation-specific exceptions (e.g. RDF4J's {@code RDFHandlerException}) so callers
 * of the {@link RdfSerializer} SPI do not need to depend on any particular backend's exception
 * hierarchy.
 */
public class RdfSerializationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RdfSerializationException(String message) {
        super(message);
    }

    public RdfSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
