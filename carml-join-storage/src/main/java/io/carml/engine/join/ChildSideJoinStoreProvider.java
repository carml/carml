package io.carml.engine.join;

import lombok.NonNull;

public interface ChildSideJoinStoreProvider<T1, T2> {

    ChildSideJoinStore<T1, T2> createChildSideJoinStore(@NonNull String name);
}
