package io.carml.engine.join.impl;

import io.carml.engine.join.ChildSideJoinStore;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import java.util.HashSet;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CarmlChildSideJoinStoreProvider<T1, T2> implements ChildSideJoinStoreProvider<T1, T2> {

    public static <T1, T2> CarmlChildSideJoinStoreProvider<T1, T2> of() {
        return new CarmlChildSideJoinStoreProvider<>();
    }

    @Override
    public ChildSideJoinStore<T1, T2> createChildSideJoinStore(@NonNull String name) {
        return CarmlChildSideJoinStore.of(name, new HashSet<>());
    }
}
