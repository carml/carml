package io.carml.engine.join.impl;

import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.engine.join.ParentSideJoinKey;
import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CarmlParentSideJoinConditionStore<T extends Serializable> implements ParentSideJoinConditionStore<T> {

  private final String name;

  private final ConcurrentMap<ParentSideJoinKey, Set<T>> parentSideJoinConditionStore;

  static <T extends Serializable> CarmlParentSideJoinConditionStore<T> of(String name,
      ConcurrentMap<ParentSideJoinKey, Set<T>> parentSideJoinConditionStore) {
    return new CarmlParentSideJoinConditionStore<>(name, parentSideJoinConditionStore);
  }

  @Override
  public void put(ParentSideJoinKey parentSideJoinKey, Set<T> values) {
    parentSideJoinConditionStore.put(parentSideJoinKey, values);
  }

  @Override
  public boolean containsKey(ParentSideJoinKey parentSideJoinKey) {
    return parentSideJoinConditionStore.containsKey(parentSideJoinKey);
  }

  @Override
  public Set<T> get(ParentSideJoinKey parentSideJoinKey) {
    return parentSideJoinConditionStore.get(parentSideJoinKey);
  }

  @Override
  public void clear() {
    parentSideJoinConditionStore.clear();
  }
}
