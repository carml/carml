package io.carml.engine;

/**
 * Indicates why a mapping execution ended.
 */
public enum CompletionReason {

    /** All source data was processed (batch mode). */
    EXHAUSTED,

    /** Execution was cancelled via {@link MappingExecution#cancel()} (streaming mode). */
    CANCELLED,

    /** A fatal error terminated execution. */
    ERROR
}
