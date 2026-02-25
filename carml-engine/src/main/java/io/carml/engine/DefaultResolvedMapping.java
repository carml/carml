package io.carml.engine;

import io.carml.model.LogicalView;
import io.carml.model.TriplesMap;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Default implementation of {@link ResolvedMapping}. Pairs a user-authored TriplesMap with the
 * effective LogicalView the engine processes and stores provenance information for each field.
 */
@Builder
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
class DefaultResolvedMapping implements ResolvedMapping {

    @NonNull
    @EqualsAndHashCode.Include
    private final TriplesMap originalTriplesMap;

    @NonNull
    private final LogicalView effectiveView;

    private final boolean implicitView;

    @NonNull
    @Getter(lombok.AccessLevel.NONE)
    @ToString.Exclude
    private final Map<String, FieldOrigin> fieldOrigins;

    @Override
    public Optional<FieldOrigin> getFieldOrigin(String fieldName) {
        return Optional.ofNullable(fieldOrigins.get(fieldName));
    }

    /**
     * Custom builder that stores an unmodifiable copy of the field origins map.
     */
    static class DefaultResolvedMappingBuilder {

        DefaultResolvedMappingBuilder fieldOrigins(Map<String, FieldOrigin> fieldOrigins) {
            this.fieldOrigins = Map.copyOf(fieldOrigins);
            return this;
        }
    }
}
