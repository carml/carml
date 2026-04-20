package io.carml.engine.rdf;

import io.carml.engine.MappedValue;
import io.carml.model.LogicalTarget;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.ToString;
import org.eclipse.rdf4j.model.Value;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
@ToString
public class RdfMappedValue<T extends Value> implements MappedValue<T> {

    private final T value;

    @Singular
    private final Set<LogicalTarget> logicalTargets;

    public static <T extends Value> MappedValue<T> of(T value) {
        return of(value, Set.of());
    }

    public static <T extends Value> MappedValue<T> of(T value, Set<LogicalTarget> logicalTargets) {
        return new RdfMappedValue<>(value, logicalTargets);
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public Set<LogicalTarget> getLogicalTargets() {
        return logicalTargets;
    }
}
