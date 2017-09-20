package com.taxonic.carml.engine;

import java.util.Optional;
import java.util.function.Function;

interface EvaluateExpression extends Function<String, Optional<Object>> {

}
