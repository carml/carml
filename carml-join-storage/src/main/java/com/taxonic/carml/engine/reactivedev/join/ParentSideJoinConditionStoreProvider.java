package com.taxonic.carml.engine.reactivedev.join;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public interface ParentSideJoinConditionStoreProvider<T> {

  ConcurrentMap<ParentSideJoinKey, Set<T>> create(String name);

}
