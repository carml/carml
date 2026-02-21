package io.carml.logicalview;

import io.carml.model.LogicalSource;
import java.util.Optional;

/**
 * Factory for creating {@link SourceIntrospector} instances that match a given
 * {@link LogicalSource}. Implementations are discovered via {@link java.util.ServiceLoader} and
 * produce a {@link MatchedSourceIntrospector} if they can handle the source's reference
 * formulation and format.
 *
 * <p>This follows the same match-and-select pattern as
 * {@link io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory}: multiple factories
 * may match a logical source with different scores, and the one with the highest score is selected
 * via {@link MatchedSourceIntrospector#select(java.util.List)}.
 */
public interface SourceIntrospectorFactory {

    /**
     * Attempts to match the given logical source. Returns a {@link MatchedSourceIntrospector} if
     * this factory can produce an introspector for the source, or empty otherwise.
     *
     * @param logicalSource the logical source to match
     * @return an optional matched introspector with its match score
     */
    Optional<MatchedSourceIntrospector> match(LogicalSource logicalSource);
}
