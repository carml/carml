package io.carml.engine;

import io.carml.model.Target;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor(staticName = "of")
@Getter
@EqualsAndHashCode
@ToString
public class MappedValue<T> {

    private final T value;

    private final Set<Target> targets;

    public static <T> MappedValue<T> of(T value) {
        return of(value, Set.of());
    }
}
