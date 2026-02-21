package io.carml.engine;

import java.time.Duration;
import java.time.Instant;

/**
 * Metrics snapshot for a {@link MappingExecution}. Provides information about the current state of
 * the execution, including the number of statements produced, errors encountered, and timing.
 */
public interface MappingExecutionMetrics {

    /**
     * The total number of RDF statements produced so far.
     *
     * @return the count of statements emitted
     */
    long statementsProduced();

    /**
     * The total number of errors encountered during the execution.
     *
     * @return the count of errors
     */
    long errorsEncountered();

    /**
     * The instant at which the execution was started.
     *
     * @return the start time
     */
    Instant startedAt();

    /**
     * The duration elapsed since the execution started.
     *
     * @return the elapsed duration
     */
    Duration elapsed();
}
