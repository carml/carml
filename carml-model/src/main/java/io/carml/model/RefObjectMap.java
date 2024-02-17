package io.carml.model;

import java.util.Set;

public interface RefObjectMap extends BaseObjectMap {

  TriplesMap getParentTriplesMap();

  Set<Join> getJoinConditions();

  default boolean isSelfJoining(TriplesMap childTriplesMap) {
    if (!childTriplesMap.getLogicalSource()
        .equals(getParentTriplesMap().getLogicalSource())) {
      return false;
    }

    return getJoinConditions().stream()
        .allMatch(join -> join.getChild()
            .equals(join.getParent()));
  }
}
