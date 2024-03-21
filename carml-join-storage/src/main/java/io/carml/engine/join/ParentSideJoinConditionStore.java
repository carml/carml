package io.carml.engine.join;

import java.util.Set;

public interface ParentSideJoinConditionStore<T> {

    void put(ParentSideJoinKey parentSideJoinKey, Set<T> values);

    boolean containsKey(ParentSideJoinKey parentSideJoinKey);

    Set<T> get(ParentSideJoinKey parentSideJoinKey);

    void clear();
}
