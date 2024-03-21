package io.carml.model;

import java.util.Set;

public interface TermMap extends ExpressionMap {

    String getInverseExpression();

    TermType getTermType();

    Set<LogicalTarget> getLogicalTargets();

    Set<Target> getTargets();
}
