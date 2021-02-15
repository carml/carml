package com.taxonic.carml.engine;

import java.util.Optional;
import java.util.function.Function;

// TODO should probably be EvaluateExpression<T> extends Function<String, Optional<T>>
public interface EvaluateExpression extends Function<String, Optional<Object>> {

}
