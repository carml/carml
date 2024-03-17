package io.carml.logicalsourceresolver;

import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Data
public class ResolvedSource<V> {

    @Getter
    @NonNull
    private Object rmlSource;

    private V resolved;

    @Getter
    private Class<V> resolvedClass;

    public static <V> ResolvedSource<V> of(Object rmlSource, V resolved, Class<V> resolvedClass) {
        return new ResolvedSource<>(rmlSource, resolved, resolvedClass);
    }

    public Optional<V> getResolved() {
        return Optional.ofNullable(resolved);
    }
}
