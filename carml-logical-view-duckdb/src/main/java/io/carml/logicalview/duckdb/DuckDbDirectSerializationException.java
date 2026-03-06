package io.carml.logicalview.duckdb;

import java.io.Serial;

/**
 * Exception thrown when DuckDB direct serialization via {@code COPY TO} fails.
 *
 * @see DuckDbDirectSerializer
 */
public class DuckDbDirectSerializationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new exception with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the underlying cause
     */
    public DuckDbDirectSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
