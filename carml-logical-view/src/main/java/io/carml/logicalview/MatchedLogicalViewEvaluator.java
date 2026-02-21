package io.carml.logicalview;

import static java.util.Comparator.comparing;

import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Wraps a {@link LogicalViewEvaluator} together with a {@link MatchScore} indicating how well the
 * evaluator matches a given {@link io.carml.model.LogicalView}. Used by
 * {@link LogicalViewEvaluatorFactory} to select the best-matching evaluator.
 */
@AllArgsConstructor(staticName = "of")
@Getter
public class MatchedLogicalViewEvaluator {

    private MatchScore matchScore;

    private LogicalViewEvaluator logicalViewEvaluator;

    /**
     * Selects the best-matching evaluator from a list of matches, based on the highest match score.
     *
     * @param matches the list of matched evaluators
     * @return the evaluator with the highest score, or empty if the list is empty
     */
    public static Optional<LogicalViewEvaluator> select(List<MatchedLogicalViewEvaluator> matches) {
        return matches.stream()
                .max(comparing(match -> match.getMatchScore().getScore()))
                .map(MatchedLogicalViewEvaluator::getLogicalViewEvaluator);
    }
}
