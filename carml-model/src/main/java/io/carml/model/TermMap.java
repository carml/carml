package io.carml.model;

public interface TermMap extends ExpressionMap {

  String getInverseExpression();

  TermType getTermType();

}
