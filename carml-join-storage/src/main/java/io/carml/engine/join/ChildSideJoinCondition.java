package io.carml.engine.join;

import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode
public class ChildSideJoinCondition {

    @NonNull
    private String childReference;

    private List<String> childValues;

    @NonNull
    private String parentReference;

    public static ChildSideJoinCondition of(String childReference, List<String> childValues, String parentReference) {
        return new ChildSideJoinCondition(childReference, childValues, parentReference);
    }
}
