package io.carml.engine;

import java.util.Optional;

/**
 * Describes an error that occurred during mapping execution. Wraps the error message, the
 * expression being evaluated (if applicable), and the underlying cause.
 *
 * @param message human-readable description of the error
 * @param expression the mapping expression being evaluated when the error occurred, or empty
 * @param cause the underlying exception, or empty
 */
public record MappingError(String message, Optional<String> expression, Optional<Throwable> cause) {

    /**
     * Creates a {@link MappingError} with a message and cause, but no expression context.
     *
     * @param message human-readable description of the error
     * @param cause the underlying exception, may be {@code null}
     * @return a new MappingError
     */
    public static MappingError of(String message, Throwable cause) {
        return new MappingError(message, Optional.empty(), Optional.ofNullable(cause));
    }

    /**
     * Creates a {@link MappingError} with a message, the expression that failed, and the cause.
     *
     * @param message human-readable description of the error
     * @param expression the mapping expression being evaluated
     * @param cause the underlying exception, may be {@code null}
     * @return a new MappingError
     */
    public static MappingError of(String message, String expression, Throwable cause) {
        return new MappingError(message, Optional.of(expression), Optional.ofNullable(cause));
    }
}
