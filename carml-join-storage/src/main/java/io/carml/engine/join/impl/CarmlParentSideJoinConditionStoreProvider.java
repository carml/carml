package io.carml.engine.join.impl;

import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.engine.join.ParentSideJoinConditionStoreProvider;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CarmlParentSideJoinConditionStoreProvider<T> implements ParentSideJoinConditionStoreProvider<T> {

    public static <T> CarmlParentSideJoinConditionStoreProvider<T> of() {
        return new CarmlParentSideJoinConditionStoreProvider<>();
    }

    @Override
    public ParentSideJoinConditionStore<T> createParentSideJoinConditionStore(@NonNull String name) {
        return CarmlParentSideJoinConditionStore.of(name, new ConcurrentHashMap<>());
    }
}
