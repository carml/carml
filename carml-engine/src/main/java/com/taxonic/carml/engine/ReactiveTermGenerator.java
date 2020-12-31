package com.taxonic.carml.engine;

import java.util.function.Function;
import reactor.core.publisher.Flux;

public interface ReactiveTermGenerator<T> extends Function<ExpressionEvaluation, Flux<T>> {

}
