package io.carml.engine.join;

import lombok.NonNull;

public interface ParentSideJoinConditionStoreProvider<T> {

    ParentSideJoinConditionStore<T> createParentSideJoinConditionStore(@NonNull String name);
}
