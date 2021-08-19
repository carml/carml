package com.taxonic.carml.engine;

import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStore;
import com.taxonic.carml.model.TriplesMap;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

public interface TriplesMapper<E, V> {

  Flux<V> map(E item);

  Flux<V> map(ExpressionEvaluation expressionEvaluation);

  TriplesMap getTriplesMap();

  Set<RefObjectMapper<V>> getRefObjectMappers();

  ParentSideJoinConditionStore<Resource> getParentSideJoinConditions();

  Mono<Void> notifyCompletion(RefObjectMapper<V> refObjectMapper, SignalType signalType);

}
