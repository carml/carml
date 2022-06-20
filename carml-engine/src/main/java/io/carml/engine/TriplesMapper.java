package io.carml.engine;

import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.model.TriplesMap;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;
import reactor.core.publisher.Flux;

public interface TriplesMapper<V> {

  Flux<V> map(LogicalSourceRecord<?> logicalSourceRecord);

  Flux<V> mapEvaluation(ExpressionEvaluation expressionEvaluation);

  TriplesMap getTriplesMap();

  Set<RefObjectMapper<V>> getRefObjectMappers();

  ParentSideJoinConditionStore<Resource> getParentSideJoinConditions();

  void cleanup();
}
