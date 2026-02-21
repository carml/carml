package io.carml.model;

import java.util.Set;

public interface LogicalViewJoin extends Resource {

    LogicalView getParentLogicalView();

    Set<Join> getJoinConditions();

    Set<ExpressionField> getFields();
}
