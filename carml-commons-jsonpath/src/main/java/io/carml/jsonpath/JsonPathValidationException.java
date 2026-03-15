package io.carml.jsonpath;

import java.io.Serial;

/**
 * Thrown when a JSONPath expression fails validation.
 *
 * <p>This is a shared exception used by both the reactive resolver and the DuckDB logical view
 * compiler. Callers should catch or wrap this exception as appropriate for their context (e.g.
 * wrapping in {@code LogicalSourceResolverException} or {@code IllegalArgumentException}).
 */
public class JsonPathValidationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public JsonPathValidationException(String message) {
        super(message);
    }

    public JsonPathValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
