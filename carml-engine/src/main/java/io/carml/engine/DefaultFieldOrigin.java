package io.carml.engine;

import io.carml.model.Field;
import io.carml.model.TermMap;
import io.carml.model.TriplesMap;
import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Default implementation of {@link FieldOrigin}. Tracks provenance from an effective view field back
 * to the original mapping constructs that produced it.
 */
@Builder
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
class DefaultFieldOrigin implements FieldOrigin {

    @NonNull
    private final String originalExpression;

    @Getter(lombok.AccessLevel.NONE)
    private final TermMap originatingTermMap;

    @NonNull
    @EqualsAndHashCode.Include
    private final TriplesMap originatingTriplesMap;

    @NonNull
    @EqualsAndHashCode.Include
    private final Field field;

    @Override
    public Optional<TermMap> getOriginatingTermMap() {
        return Optional.ofNullable(originatingTermMap);
    }
}
