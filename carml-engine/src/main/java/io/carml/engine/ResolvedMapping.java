package io.carml.engine;

import io.carml.logicalview.EvaluationContext;
import io.carml.model.LogicalView;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Pairs a user-authored TriplesMap with the effective LogicalView the engine processes. Provides
 * provenance tracking from internal field names back to original mapping constructs.
 *
 * <p>For explicit LogicalView mappings (RML-LV), the effective view is the user's own view. For bare
 * LogicalSource mappings (RML-Core), the effective view is a synthetic view wrapping the bare
 * LogicalSource, with auto-derived fields for each referenced expression.
 */
public interface ResolvedMapping {

    /**
     * Creates a ResolvedMapping from a TriplesMap with the given effective LogicalView.
     *
     * @param originalTriplesMap the TriplesMap as authored by the user
     * @param effectiveView the effective LogicalView the engine processes
     * @param implicitView whether the effective view was synthetically derived from a bare
     *     LogicalSource
     * @param fieldOrigins mapping from field names to their provenance information
     * @param evaluationContext the evaluation context controlling field projection and limits
     * @return a new ResolvedMapping instance
     */
    static ResolvedMapping of(
            TriplesMap originalTriplesMap,
            LogicalView effectiveView,
            boolean implicitView,
            Map<String, FieldOrigin> fieldOrigins,
            EvaluationContext evaluationContext) {
        return of(originalTriplesMap, effectiveView, implicitView, fieldOrigins, evaluationContext, Set.of());
    }

    /**
     * Creates a ResolvedMapping from a TriplesMap with the given effective LogicalView and
     * dependency information.
     *
     * @param originalTriplesMap the TriplesMap as authored by the user
     * @param effectiveView the effective LogicalView the engine processes
     * @param implicitView whether the effective view was synthetically derived from a bare
     *     LogicalSource
     * @param fieldOrigins mapping from field names to their provenance information
     * @param evaluationContext the evaluation context controlling field projection and limits
     * @param dependencies the set of TriplesMaps that this mapping depends on via RefObjectMaps
     * @return a new ResolvedMapping instance
     */
    static ResolvedMapping of(
            TriplesMap originalTriplesMap,
            LogicalView effectiveView,
            boolean implicitView,
            Map<String, FieldOrigin> fieldOrigins,
            EvaluationContext evaluationContext,
            Set<TriplesMap> dependencies) {
        return DefaultResolvedMapping.builder()
                .originalTriplesMap(originalTriplesMap)
                .effectiveView(effectiveView)
                .implicitView(implicitView)
                .fieldOrigins(fieldOrigins)
                .evaluationContext(evaluationContext)
                .dependencies(dependencies)
                .build();
    }

    /**
     * Creates a ResolvedMapping from a TriplesMap with the given effective LogicalView, dependency
     * information, and RefObjectMap prefix mappings.
     *
     * @param originalTriplesMap the TriplesMap as authored by the user
     * @param effectiveView the effective LogicalView the engine processes
     * @param implicitView whether the effective view was synthetically derived from a bare
     *     LogicalSource
     * @param fieldOrigins mapping from field names to their provenance information
     * @param evaluationContext the evaluation context controlling field projection and limits
     * @param dependencies the set of TriplesMaps that this mapping depends on via RefObjectMaps
     * @param refObjectMapPrefixes mapping from each handled RefObjectMap to its expression prefix
     * @return a new ResolvedMapping instance
     */
    static ResolvedMapping of(
            TriplesMap originalTriplesMap,
            LogicalView effectiveView,
            boolean implicitView,
            Map<String, FieldOrigin> fieldOrigins,
            EvaluationContext evaluationContext,
            Set<TriplesMap> dependencies,
            Map<RefObjectMap, String> refObjectMapPrefixes) {
        return DefaultResolvedMapping.builder()
                .originalTriplesMap(originalTriplesMap)
                .effectiveView(effectiveView)
                .implicitView(implicitView)
                .fieldOrigins(fieldOrigins)
                .evaluationContext(evaluationContext)
                .dependencies(dependencies)
                .refObjectMapPrefixes(refObjectMapPrefixes)
                .build();
    }

    /**
     * Creates a ResolvedMapping for a decomposed view group, with a subset of predicate-object maps
     * and a flag controlling class triple emission.
     *
     * @param originalTriplesMap the TriplesMap as authored by the user
     * @param effectiveView the effective LogicalView the engine processes
     * @param fieldOrigins mapping from field names to their provenance information
     * @param evaluationContext the evaluation context controlling field projection and limits
     * @param dependencies the set of TriplesMaps that this mapping depends on via RefObjectMaps
     * @param activePredicateObjectMaps the subset of POMs for this decomposition group; empty means
     *     all
     * @param emitsClassTriples whether this group should emit rdf:type class triples
     * @return a new ResolvedMapping instance
     */
    static ResolvedMapping of(
            TriplesMap originalTriplesMap,
            LogicalView effectiveView,
            Map<String, FieldOrigin> fieldOrigins,
            EvaluationContext evaluationContext,
            Set<TriplesMap> dependencies,
            Set<PredicateObjectMap> activePredicateObjectMaps,
            boolean emitsClassTriples) {
        return DefaultResolvedMapping.builder()
                .originalTriplesMap(originalTriplesMap)
                .effectiveView(effectiveView)
                .implicitView(false)
                .fieldOrigins(fieldOrigins)
                .evaluationContext(evaluationContext)
                .dependencies(dependencies)
                .activePredicateObjectMaps(activePredicateObjectMaps)
                .emitsClassTriples(emitsClassTriples)
                .build();
    }

    /**
     * The TriplesMap as authored by the user. Used for error reporting and diagnostics.
     */
    TriplesMap getOriginalTriplesMap();

    /**
     * The effective LogicalView that the engine processes. For explicit LV mappings, this is the
     * user's view. For bare LogicalSource mappings, this is a synthetic view.
     */
    LogicalView getEffectiveView();

    /**
     * Whether the effective view was explicitly authored or synthetically derived from a bare
     * LogicalSource.
     */
    boolean isImplicitView();

    /**
     * The evaluation context controlling field projection, deduplication, and result limiting for
     * this mapping.
     */
    EvaluationContext getEvaluationContext();

    /**
     * Get the origin of a field in the effective view. Used to produce user-facing error messages
     * that reference the original mapping constructs.
     *
     * @param fieldName the field name to look up
     * @return the provenance information for the field, or empty if no origin is recorded for the
     *     given field name
     */
    Optional<FieldOrigin> getFieldOrigin(String fieldName);

    /**
     * The set of TriplesMaps that this mapping depends on via RefObjectMaps. Dependencies are other
     * TriplesMaps in the input set that are referenced as parent TriplesMaps. Used for
     * topological ordering and future RML-star support.
     *
     * @return the set of dependency TriplesMaps, or an empty set if there are no dependencies
     */
    default Set<TriplesMap> getDependencies() {
        return Set.of();
    }

    /**
     * Returns the mapping from RefObjectMaps to their expression prefixes. For implicit views, each
     * RefObjectMap with join conditions is assigned a prefix (e.g. {@code "_ref0."}) that
     * corresponds to a left join on the synthetic view. The prefix is used by term generators to
     * resolve parent-side expressions from the joined view iteration.
     *
     * @return the RefObjectMap prefix mapping, or an empty map if no prefixes are assigned
     */
    default Map<RefObjectMap, String> getRefObjectMapPrefixes() {
        return Map.of();
    }

    /**
     * Returns the subset of predicate-object maps that this resolved mapping should evaluate. An
     * empty set indicates all POMs from the original TriplesMap should be evaluated.
     *
     * <p>Non-empty when this mapping represents a decomposition group containing only a subset of
     * the original TriplesMap's POMs.
     *
     * @return the active POM subset, or an empty set if all POMs are active
     */
    default Set<PredicateObjectMap> getActivePredicateObjectMaps() {
        return Set.of();
    }

    /**
     * Whether this resolved mapping should emit rdf:type class triples for the subject. When a
     * TriplesMap is decomposed, only one group (the narrowest determinant) emits class triples to
     * avoid duplicates.
     *
     * @return {@code true} if class triples should be emitted, {@code false} otherwise
     */
    default boolean emitsClassTriples() {
        return true;
    }
}
