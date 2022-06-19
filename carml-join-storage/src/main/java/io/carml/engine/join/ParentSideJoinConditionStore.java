package io.carml.engine.join;

import java.io.Serializable;
import java.util.Set;

public interface ParentSideJoinConditionStore<T extends Serializable> {

  void put(ParentSideJoinKey parentSideJoinKey, Set<T> values);

  boolean containsKey(ParentSideJoinKey parentSideJoinKey);

  Set<T> get(ParentSideJoinKey parentSideJoinKey);

  void clear();
}
