package com.taxonic.carml.engine.reactivedev.join.impl;

import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStore;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CarmlParentSideJoinConditionStoreProvider<T extends Serializable>
    implements ParentSideJoinConditionStoreProvider<T> {

  public static <T extends Serializable> CarmlParentSideJoinConditionStoreProvider<T> of() {
    return new CarmlParentSideJoinConditionStoreProvider<>();
  }

  @Override
  public ParentSideJoinConditionStore<T> createParentSideJoinConditionStore(@NonNull String name) {
    return CarmlParentSideJoinConditionStore.of(name, new ConcurrentHashMap<>());
  }
}
