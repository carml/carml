package io.carml.logicalsourceresolver;

import io.carml.util.TypeRef;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Data
public class ResolvedSource<V> {

    private V resolved;

    @Getter
    private TypeRef<V> resolvedTypeRef;

    public static <V> ResolvedSource<V> of(V resolved, TypeRef<V> resolvedTypeRef) {
        return new ResolvedSource<>(resolved, resolvedTypeRef);
    }

    public Optional<V> getResolved() {
        return Optional.ofNullable(resolved);
    }
}
