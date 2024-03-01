package io.carml.model;

import java.util.function.UnaryOperator;

public interface PredicateMap extends TermMap {

  PredicateMap applyExpressionAdapter(UnaryOperator<String> referenceExpressionAdapter);
}
