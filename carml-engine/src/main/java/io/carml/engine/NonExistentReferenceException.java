package io.carml.engine;

import java.io.Serial;

/**
 * Exception thrown when strict mode is enabled and one or more reference expressions in a mapping
 * never produce a non-null result across all records of a logical source. This indicates that the
 * reference expressions are likely invalid or do not match the data source schema.
 */
public class NonExistentReferenceException extends TriplesMapperException {

    @Serial
    private static final long serialVersionUID = -4201574893219087265L;

    public NonExistentReferenceException(String message) {
        super(message);
    }

    public NonExistentReferenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
