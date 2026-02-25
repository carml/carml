package io.carml.engine;

import java.time.Duration;

/**
 * Periodic summary of execution progress, emitted via
 * {@link MappingExecutionObserver#onCheckpoint} during long-running or streaming executions.
 * Provides both incremental (since-last-checkpoint) and cumulative totals.
 *
 * @param statementsGeneratedSinceLastCheckpoint statements generated since the previous checkpoint
 * @param totalStatementsGenerated cumulative statements generated since mapping start
 * @param iterationsProcessedSinceLastCheckpoint iterations processed since the previous checkpoint
 * @param totalIterationsProcessed cumulative iterations processed since mapping start
 * @param timeSinceLastCheckpoint wall-clock time since the previous checkpoint
 * @param totalDuration wall-clock time since mapping start
 */
public record CheckpointInfo(
        long statementsGeneratedSinceLastCheckpoint,
        long totalStatementsGenerated,
        long iterationsProcessedSinceLastCheckpoint,
        long totalIterationsProcessed,
        Duration timeSinceLastCheckpoint,
        Duration totalDuration) {}
