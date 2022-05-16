package com.taxonic.carml.engine.join.impl;

import com.taxonic.carml.engine.join.ChildSideJoinStore;
import com.taxonic.carml.engine.join.ChildSideJoinStoreProvider;
import java.io.Serializable;
import java.util.HashSet;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CarmlChildSideJoinStoreProvider<T1 extends Serializable, T2 extends Serializable>
    implements ChildSideJoinStoreProvider<T1, T2> {

  public static <T1 extends Serializable, T2 extends Serializable> CarmlChildSideJoinStoreProvider<T1, T2> of() {
    return new CarmlChildSideJoinStoreProvider<>();
  }

  @Override
  public ChildSideJoinStore<T1, T2> createChildSideJoinStore(@NonNull String name) {
    return CarmlChildSideJoinStore.of(name, new HashSet<>());
  }
}
