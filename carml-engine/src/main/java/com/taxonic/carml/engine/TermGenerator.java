package com.taxonic.carml.engine;

import java.util.List;
import java.util.function.Function;

public interface TermGenerator<T> extends Function<ExpressionEvaluation, List<T>> {

}
