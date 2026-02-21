package io.carml.logicalview;

import static java.util.Comparator.comparing;

import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Wraps a {@link SourceIntrospector} together with a {@link MatchScore} indicating how well the
 * introspector matches a given {@link io.carml.model.LogicalSource}. Used by
 * {@link SourceIntrospectorFactory} to select the best-matching introspector.
 */
@AllArgsConstructor(staticName = "of")
@Getter
public class MatchedSourceIntrospector {

    private MatchScore matchScore;

    private SourceIntrospector sourceIntrospector;

    /**
     * Selects the best-matching introspector from a list of matches, based on the highest match
     * score.
     *
     * @param matches the list of matched introspectors
     * @return the introspector with the highest score, or empty if the list is empty
     */
    public static Optional<SourceIntrospector> select(List<MatchedSourceIntrospector> matches) {
        return matches.stream()
                .max(comparing(match -> match.getMatchScore().getScore()))
                .map(MatchedSourceIntrospector::getSourceIntrospector);
    }
}
