package io.carml.logicalview;

import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.Source;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import reactor.core.publisher.Flux;

/**
 * Evaluates a {@link LogicalView} definition, producing a reactive stream of
 * {@link ViewIteration}s. The evaluator resolves the view's underlying source(s) and applies field
 * projections, joins, and deduplication according to the provided {@link EvaluationContext}.
 */
public interface LogicalViewEvaluator {

    /**
     * Evaluates the given logical view, producing a flux of view iterations.
     *
     * @param view the logical view to evaluate
     * @param sourceResolver a function that resolves a {@link Source} to a {@link ResolvedSource}
     * @param context the evaluation context controlling projection, dedup, and limits
     * @return a flux of view iterations
     */
    Flux<ViewIteration> evaluate(
            LogicalView view, Function<Source, ResolvedSource<?>> sourceResolver, EvaluationContext context);

    /**
     * Evaluates the given logical view with access to a shared source record cache. When the cache
     * is active, resolved source records are stored per {@link Source} and shared across views and
     * join parents that reference the same source, avoiding redundant source parsing.
     *
     * <p>The default implementation ignores the cache and delegates to
     * {@link #evaluate(LogicalView, Function, EvaluationContext)}.
     *
     * @param view the logical view to evaluate
     * @param sourceResolver a function that resolves a {@link Source} to a {@link ResolvedSource}
     * @param context the evaluation context controlling projection, dedup, and limits
     * @param recordCache the shared source record cache
     * @param logicalSourcesPerSource the full set of LogicalSources per Source across all views
     * @param expressionsPerLogicalSource the merged expressions per LogicalSource across all views
     * @return a flux of view iterations
     */
    default Flux<ViewIteration> evaluate(
            LogicalView view,
            Function<Source, ResolvedSource<?>> sourceResolver,
            EvaluationContext context,
            SourceRecordCache recordCache,
            Map<Source, Set<LogicalSource>> logicalSourcesPerSource,
            Map<LogicalSource, Set<String>> expressionsPerLogicalSource) {
        return evaluate(view, sourceResolver, context);
    }
}
