package io.carml.model;

import java.util.function.UnaryOperator;

public interface GraphMap extends TermMap {

  GraphMap applyExpressionAdapter(UnaryOperator<String> referenceExpressionAdapter);
}
