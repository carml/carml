package io.carml.logicalview;

import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.LogicalView;
import io.carml.model.Source;
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
}
