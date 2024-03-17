package io.carml.model;

import java.util.function.UnaryOperator;

public interface ObjectMap extends TermMap, BaseObjectMap {

    DatatypeMap getDatatypeMap();

    LanguageMap getLanguageMap();

    ObjectMap applyExpressionAdapter(UnaryOperator<String> referenceExpressionAdapter);
}
