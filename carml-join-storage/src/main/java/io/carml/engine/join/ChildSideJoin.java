package io.carml.engine.join;

import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Getter
@EqualsAndHashCode
public class ChildSideJoin<T1, T2> {

    private final Set<T1> subjects;

    private final Set<T2> predicates;

    private final Set<T1> graphs;

    private final Set<ChildSideJoinCondition> childSideJoinConditions;
}
