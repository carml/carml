package com.taxonic.carml.engine;

import com.taxonic.carml.model.TriplesMap;
import reactor.core.publisher.Flux;

public interface TriplesMapper<E, V> {

  Flux<V> map(E item);

  Flux<V> map(ExpressionEvaluation expressionEvaluation);

  TriplesMap getTriplesMap();

}
