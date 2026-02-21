package io.carml.logicalview;

import io.carml.model.LogicalView;
import java.util.Optional;

/**
 * Factory for creating {@link LogicalViewEvaluator} instances that match a given
 * {@link LogicalView}. Implementations are discovered via {@link java.util.ServiceLoader} and
 * produce a {@link MatchedLogicalViewEvaluator} if they can handle the view.
 */
public interface LogicalViewEvaluatorFactory {

    /**
     * Attempts to match the given logical view. Returns a {@link MatchedLogicalViewEvaluator} if
     * this factory can produce an evaluator for the view, or empty otherwise.
     *
     * @param view the logical view to match
     * @return an optional matched evaluator with its match score
     */
    Optional<MatchedLogicalViewEvaluator> match(LogicalView view);
}
