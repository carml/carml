package io.carml.engine.join;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode
@ToString
public class ParentSideJoinKey {

    @NonNull
    private String parentReference;

    @NonNull
    private String parentValue;

    public static ParentSideJoinKey of(@NonNull String parentReference, @NonNull String parentValue) {
        return new ParentSideJoinKey(parentReference, parentValue);
    }
}
