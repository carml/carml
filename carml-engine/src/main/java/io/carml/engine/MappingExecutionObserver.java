package io.carml.engine;

import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import java.time.Duration;
import java.util.Set;
import org.eclipse.rdf4j.model.Statement;

/**
 * Observer SPI for mapping execution events. Implementations receive callbacks at key points in the
 * pipeline: view evaluation, RDF generation, deduplication, and error handling.
 *
 * <p>Multiple observers can be composed via {@code CompositeObserver}. When no observer is
 * configured, a no-op implementation is used (zero overhead).
 *
 * <p>Implementations <b>MUST NOT</b> accumulate unbounded data structures across invocations of
 * {@link #onStatementGenerated} or {@link #onViewIteration}. These callbacks may be invoked
 * billions of times in batch mode or indefinitely in streaming mode. Use counters, gauges, and
 * periodic flush (via {@link #onCheckpoint}) instead.
 *
 * <p>Implementations <b>MUST</b> be thread-safe. Callbacks may be invoked from different reactive
 * threads. Keep callback implementations lightweight and non-blocking to avoid introducing latency
 * into the mapping pipeline.
 *
 * <p>All methods have default no-op implementations, allowing observers to override only the
 * callbacks they are interested in.
 */
public interface MappingExecutionObserver {

    // --- Mapping lifecycle ---

    /**
     * Called once when the mapper starts processing a {@link ResolvedMapping}.
     *
     * @param mapping the resolved mapping being processed
     */
    default void onMappingStart(ResolvedMapping mapping) {}

    /**
     * Called once when the mapper finishes processing a {@link ResolvedMapping}. For batch mode this
     * is called when all data is exhausted. For streaming mode this is called when execution is
     * canceled or stopped. Check {@link MappingExecutionResult#completionReason()} for the cause.
     *
     * @param mapping the resolved mapping that completed
     * @param result summary of the mapping execution
     */
    default void onMappingComplete(ResolvedMapping mapping, MappingExecutionResult result) {}

    /**
     * Called periodically during long-running or streaming executions. Provides a rolling summary of
     * execution progress. Not called for short batch executions (only
     * {@link #onMappingComplete} fires).
     *
     * @param mapping the resolved mapping being processed
     * @param checkpoint the periodic execution summary
     */
    default void onCheckpoint(ResolvedMapping mapping, CheckpointInfo checkpoint) {}

    // --- View evaluation ---

    /**
     * Called when a {@link LogicalViewEvaluator} is selected for a view.
     *
     * @param mapping the resolved mapping being processed
     * @param evaluator the evaluator selected for the mapping's effective view
     */
    default void onViewEvaluationStart(ResolvedMapping mapping, LogicalViewEvaluator evaluator) {}

    /**
     * Called for each {@link ViewIteration} produced by the evaluator.
     *
     * @param mapping the resolved mapping being processed
     * @param iteration the view iteration produced
     */
    default void onViewIteration(ResolvedMapping mapping, ViewIteration iteration) {}

    /**
     * Called when view evaluation completes.
     *
     * @param mapping the resolved mapping being processed
     * @param iterationCount total number of iterations produced
     * @param duration wall-clock duration of the view evaluation
     */
    default void onViewEvaluationComplete(ResolvedMapping mapping, long iterationCount, Duration duration) {}

    // --- RDF generation ---

    /**
     * Called for each RDF statement generated from a {@link ViewIteration}.
     *
     * <p>The {@code logicalTargets} set is the union of the {@link LogicalTarget}s declared on the
     * subject, predicate, object and (optional) graph term maps that produced the statement. An
     * empty set indicates the statement has no explicit logical target and should be routed to the
     * default output.
     *
     * @param mapping the resolved mapping being processed
     * @param source the view iteration that produced the statement
     * @param statement the generated RDF statement
     * @param logicalTargets the union of logical targets declared on the term maps that produced
     *     the statement
     */
    default void onStatementGenerated(
            ResolvedMapping mapping, ViewIteration source, Statement statement, Set<LogicalTarget> logicalTargets) {}

    /**
     * Called when a {@link ViewIteration} is skipped by deduplication.
     *
     * @param mapping the resolved mapping being processed
     * @param iteration the iteration that was deduplicated
     */
    default void onIterationDeduplicated(ResolvedMapping mapping, ViewIteration iteration) {}

    // --- Errors ---

    /**
     * Called when an error occurs during mapping execution.
     *
     * @param mapping the resolved mapping being processed
     * @param iteration the view iteration being processed when the error occurred, may be
     *     {@code null} if the error occurred outside iteration processing
     * @param error the error details
     */
    default void onError(ResolvedMapping mapping, ViewIteration iteration, MappingError error) {}
}
