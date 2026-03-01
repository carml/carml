package io.carml.engine;

import io.carml.logicalview.EvaluationContext;
import io.carml.model.LogicalView;
import io.carml.model.TriplesMap;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private final EvaluationContext evaluationContext;

    @NonNull
    @Getter(lombok.AccessLevel.NONE)
    @ToString.Exclude
    private final Map<String, FieldOrigin> fieldOrigins;

    @NonNull
    private final Set<TriplesMap> dependencies;

    @Override
    public Optional<FieldOrigin> getFieldOrigin(String fieldName) {
        return Optional.ofNullable(fieldOrigins.get(fieldName));
    }

    /**
     * Custom builder that stores unmodifiable copies of the field origins map and dependencies set.
     */
    static class DefaultResolvedMappingBuilder {

        DefaultResolvedMappingBuilder fieldOrigins(Map<String, FieldOrigin> fieldOrigins) {
            this.fieldOrigins = Map.copyOf(fieldOrigins);
            return this;
        }

        DefaultResolvedMappingBuilder dependencies(Set<TriplesMap> dependencies) {
            this.dependencies = Collections.unmodifiableSet(new LinkedHashSet<>(dependencies));
            return this;
        }
    }
}
