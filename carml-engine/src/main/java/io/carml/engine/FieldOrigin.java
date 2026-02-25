package io.carml.engine;

import io.carml.model.Field;
import io.carml.model.TermMap;
import io.carml.model.TriplesMap;
import java.util.Optional;

/**
 * Provenance information for a field in the effective LogicalView. Enables error messages that
 * reference the user's original mapping rather than internal synthetic constructs.
 */
public interface FieldOrigin {

    /**
     * Creates a FieldOrigin for an explicit view field (no originating TermMap).
     */
    static FieldOrigin of(String originalExpression, TriplesMap originatingTriplesMap, Field field) {
        return DefaultFieldOrigin.builder()
                .originalExpression(originalExpression)
                .originatingTriplesMap(originatingTriplesMap)
                .field(field)
                .build();
    }

    /**
     * Creates a FieldOrigin for a synthetic field derived from a TermMap expression.
     */
    static FieldOrigin of(
            String originalExpression, TermMap originatingTermMap, TriplesMap originatingTriplesMap, Field field) {
        return DefaultFieldOrigin.builder()
                .originalExpression(originalExpression)
                .originatingTermMap(originatingTermMap)
                .originatingTriplesMap(originatingTriplesMap)
                .field(field)
                .build();
    }

    /**
     * The original expression as written by the user. For explicit views: the field's rml:reference
     * value. For implicit views: the raw source expression (e.g. "$.name", "//student/name",
     * "Name").
     */
    String getOriginalExpression();

    /**
     * The TermMap that caused this field to be created. Empty for explicit view fields (they exist
     * independently of any TermMap).
     */
    Optional<TermMap> getOriginatingTermMap();

    /**
     * The TriplesMap context. Always present.
     */
    TriplesMap getOriginatingTriplesMap();

    /**
     * The field definition in the effective view.
     */
    Field getField();
}
