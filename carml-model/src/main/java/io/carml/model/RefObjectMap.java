package io.carml.model;

import java.util.Set;

public interface RefObjectMap extends BaseObjectMap {

    TriplesMap getParentTriplesMap();

    Set<Join> getJoinConditions();

    default boolean isSelfJoining(TriplesMap childTriplesMap) {
        if (!childTriplesMap.getLogicalSource().equals(getParentTriplesMap().getLogicalSource())
                || getParentTriplesMap().equals(childTriplesMap)) {
            return false;
        }

        // TODO check after childmap parentmap introduction
        return getJoinConditions().stream().allMatch(join -> join.getChildMap()
                .getExpressionMapExpressionSet()
                .equals(join.getParentMap().getExpressionMapExpressionSet()));
    }
}
