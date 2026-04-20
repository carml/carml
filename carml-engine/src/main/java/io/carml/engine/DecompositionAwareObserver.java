package io.carml.engine;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import io.carml.model.TriplesMap;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Statement;

/**
 * Wraps a {@link MappingExecutionObserver} and aggregates callbacks across sub-mappings when view
 * decomposition is active. A decomposed TriplesMap with N sub-groups produces the same observer
 * event sequence as a non-decomposed one: exactly one {@code onMappingStart}, one
 * {@code onMappingComplete}, one {@code onViewEvaluationStart}, and one
 * {@code onViewEvaluationComplete} per original TriplesMap.
 *
 * <p>Only TriplesMap instances with multiple sub-groups are aggregated. TriplesMap instances with a
 * single resolved mapping pass through directly with zero overhead.
 *
 * <p>All forwarded callbacks use the primary ResolvedMapping (the one with
 * {@code emitsClassTriples() == true}) so the downstream observer sees a single consistent
 * ResolvedMapping per original TriplesMap.
 */
@Slf4j
public final class DecompositionAwareObserver implements MappingExecutionObserver {

    private final MappingExecutionObserver delegate;

    /**
     * Number of sub-groups per original TriplesMap. Only entries with count > 1 need aggregation.
     */
    private final Map<TriplesMap, Integer> subGroupCounts;

    /**
     * The primary (class-triple-emitting) ResolvedMapping per original TriplesMap.
     */
    private final Map<TriplesMap, ResolvedMapping> primaryMappings;

    // --- Tracking state per original TriplesMap ---
    // Sub-groups of a single TriplesMap are processed sequentially by the engine,
    // so plain (non-thread-safe) collections are sufficient.

    private final Map<TriplesMap, Integer> mappingStartCount = new HashMap<>();
    private final Map<TriplesMap, Integer> mappingCompleteCount = new HashMap<>();
    private final Map<TriplesMap, Integer> viewEvalStartCount = new HashMap<>();
    private final Map<TriplesMap, Integer> viewEvalCompleteCount = new HashMap<>();

    private final Map<TriplesMap, Long> aggregatedStatements = new HashMap<>();
    private final Map<TriplesMap, Long> aggregatedIterations = new HashMap<>();
    private final Map<TriplesMap, Long> aggregatedDeduplicated = new HashMap<>();
    private final Map<TriplesMap, Long> aggregatedErrors = new HashMap<>();
    private final Map<TriplesMap, Duration> aggregatedMaxDuration = new HashMap<>();
    private final Map<TriplesMap, CompletionReason> aggregatedWorstReason = new HashMap<>();

    private final Map<TriplesMap, Long> aggregatedViewIterations = new HashMap<>();
    private final Map<TriplesMap, Duration> aggregatedViewMaxDuration = new HashMap<>();

    private DecompositionAwareObserver(
            MappingExecutionObserver delegate,
            Map<TriplesMap, Integer> subGroupCounts,
            Map<TriplesMap, ResolvedMapping> primaryMappings) {
        this.delegate = delegate;
        this.subGroupCounts = subGroupCounts;
        this.primaryMappings = primaryMappings;
    }

    /**
     * Creates a decomposition-aware wrapper if any TriplesMap was decomposed into multiple groups.
     * Returns the original observer unchanged when no decomposition occurred.
     *
     * @param observer the observer to wrap
     * @param resolvedMappings the full list of resolved mappings (may contain decomposed sub-groups)
     * @return the original observer if no decomposition, or a wrapping aggregator otherwise
     */
    public static MappingExecutionObserver wrap(
            MappingExecutionObserver observer, List<ResolvedMapping> resolvedMappings) {
        var counts = resolvedMappings.stream().collect(groupingBy(ResolvedMapping::getOriginalTriplesMap, counting()));

        var hasDecomposition = counts.values().stream().anyMatch(c -> c > 1);
        if (!hasDecomposition) {
            return observer;
        }

        var intCounts = new HashMap<TriplesMap, Integer>();
        counts.forEach((tm, count) -> intCounts.put(tm, count.intValue()));

        var primaries = new HashMap<TriplesMap, ResolvedMapping>();
        for (var rm : resolvedMappings) {
            if (rm.emitsClassTriples()) {
                primaries.putIfAbsent(rm.getOriginalTriplesMap(), rm);
            }
        }

        return new DecompositionAwareObserver(observer, intCounts, primaries);
    }

    private boolean isAggregated(ResolvedMapping mapping) {
        return subGroupCounts.getOrDefault(mapping.getOriginalTriplesMap(), 1) > 1;
    }

    private ResolvedMapping primaryOf(ResolvedMapping mapping) {
        return primaryMappings.getOrDefault(mapping.getOriginalTriplesMap(), mapping);
    }

    @Override
    public void onMappingStart(ResolvedMapping mapping) {
        if (!isAggregated(mapping)) {
            delegate.onMappingStart(mapping);
            return;
        }

        var count = mappingStartCount.merge(mapping.getOriginalTriplesMap(), 1, Integer::sum);
        if (count == 1) {
            delegate.onMappingStart(primaryOf(mapping));
        }
    }

    @Override
    public void onMappingComplete(ResolvedMapping mapping, MappingExecutionResult result) {
        if (!isAggregated(mapping)) {
            delegate.onMappingComplete(mapping, result);
            return;
        }

        var tm = mapping.getOriginalTriplesMap();
        aggregatedStatements.merge(tm, result.statementsGenerated(), Long::sum);
        aggregatedIterations.merge(tm, result.iterationsProcessed(), Long::sum);
        aggregatedDeduplicated.merge(tm, result.iterationsDeduplicated(), Long::sum);
        aggregatedErrors.merge(tm, result.errorsEncountered(), Long::sum);
        aggregatedMaxDuration.merge(tm, result.duration(), (a, b) -> a.compareTo(b) >= 0 ? a : b);
        aggregatedWorstReason.merge(tm, result.completionReason(), (a, b) -> a.ordinal() > b.ordinal() ? a : b);

        var count = mappingCompleteCount.merge(tm, 1, Integer::sum);
        var expected = subGroupCounts.getOrDefault(tm, 1);
        if (count >= expected) {
            var aggregatedResult = new MappingExecutionResult(
                    aggregatedStatements.get(tm),
                    aggregatedIterations.get(tm),
                    aggregatedDeduplicated.get(tm),
                    aggregatedErrors.get(tm),
                    aggregatedMaxDuration.get(tm),
                    aggregatedWorstReason.get(tm));
            delegate.onMappingComplete(primaryOf(mapping), aggregatedResult);
        }
    }

    @Override
    public void onCheckpoint(ResolvedMapping mapping, CheckpointInfo checkpoint) {
        // Forward as-is using primary mapping for consistency
        delegate.onCheckpoint(isAggregated(mapping) ? primaryOf(mapping) : mapping, checkpoint);
    }

    @Override
    public void onViewEvaluationStart(ResolvedMapping mapping, LogicalViewEvaluator evaluator) {
        if (!isAggregated(mapping)) {
            delegate.onViewEvaluationStart(mapping, evaluator);
            return;
        }

        var count = viewEvalStartCount.merge(mapping.getOriginalTriplesMap(), 1, Integer::sum);
        if (count == 1) {
            delegate.onViewEvaluationStart(primaryOf(mapping), evaluator);
        }
    }

    @Override
    public void onViewIteration(ResolvedMapping mapping, ViewIteration iteration) {
        delegate.onViewIteration(isAggregated(mapping) ? primaryOf(mapping) : mapping, iteration);
    }

    @Override
    public void onViewEvaluationComplete(ResolvedMapping mapping, long iterationCount, Duration duration) {
        if (!isAggregated(mapping)) {
            delegate.onViewEvaluationComplete(mapping, iterationCount, duration);
            return;
        }

        var tm = mapping.getOriginalTriplesMap();
        aggregatedViewIterations.merge(tm, iterationCount, Long::sum);
        aggregatedViewMaxDuration.merge(tm, duration, (a, b) -> a.compareTo(b) >= 0 ? a : b);

        var count = viewEvalCompleteCount.merge(tm, 1, Integer::sum);
        var expected = subGroupCounts.getOrDefault(tm, 1);
        if (count >= expected) {
            delegate.onViewEvaluationComplete(
                    primaryOf(mapping), aggregatedViewIterations.get(tm), aggregatedViewMaxDuration.get(tm));
        }
    }

    @Override
    public void onStatementGenerated(
            ResolvedMapping mapping, ViewIteration source, Statement statement, Set<LogicalTarget> logicalTargets) {
        // mapping may be null on the post-merge firing path (see RdfRmlMapper#wrapMergedForObserver)
        // pass through unchanged since there is no sub-group to aggregate over.
        if (mapping == null) {
            delegate.onStatementGenerated(null, source, statement, logicalTargets);
            return;
        }
        delegate.onStatementGenerated(
                isAggregated(mapping) ? primaryOf(mapping) : mapping, source, statement, logicalTargets);
    }

    @Override
    public void onIterationDeduplicated(ResolvedMapping mapping, ViewIteration iteration) {
        delegate.onIterationDeduplicated(isAggregated(mapping) ? primaryOf(mapping) : mapping, iteration);
    }

    @Override
    public void onError(ResolvedMapping mapping, ViewIteration iteration, MappingError error) {
        delegate.onError(isAggregated(mapping) ? primaryOf(mapping) : mapping, iteration, error);
    }
}
