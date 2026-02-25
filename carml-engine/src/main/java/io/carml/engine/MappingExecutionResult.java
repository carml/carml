package io.carml.engine;

import java.time.Duration;

/**
 * Summary of a completed mapping execution for a single {@link ResolvedMapping}. Emitted as part of
 * the {@link MappingExecutionObserver#onMappingComplete} callback.
 *
 * <p>Note: this is distinct from {@link MappingResult}, which represents target-routed mapping
 * output during pipeline execution.
 *
 * @param statementsGenerated total number of RDF statements generated
 * @param iterationsProcessed total number of {@link io.carml.logicalview.ViewIteration}s processed
 * @param iterationsDeduplicated number of iterations eliminated by deduplication
 * @param errorsEncountered number of errors encountered during execution
 * @param duration wall-clock duration of the mapping execution
 * @param completionReason why the mapping execution ended
 */
public record MappingExecutionResult(
        long statementsGenerated,
        long iterationsProcessed,
        long iterationsDeduplicated,
        long errorsEncountered,
        Duration duration,
        CompletionReason completionReason) {}
