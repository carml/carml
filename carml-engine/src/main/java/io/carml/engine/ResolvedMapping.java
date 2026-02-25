package io.carml.engine;

import io.carml.logicalview.EvaluationContext;
import io.carml.model.LogicalView;
import io.carml.model.TriplesMap;
import java.util.Map;
import java.util.Optional;

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
        return DefaultResolvedMapping.builder()
                .originalTriplesMap(originalTriplesMap)
                .effectiveView(effectiveView)
                .implicitView(implicitView)
                .fieldOrigins(fieldOrigins)
                .evaluationContext(evaluationContext)
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
}
