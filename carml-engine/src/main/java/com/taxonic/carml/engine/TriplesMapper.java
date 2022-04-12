package com.taxonic.carml.engine;

import com.taxonic.carml.engine.join.ParentSideJoinConditionStore;
import com.taxonic.carml.logicalsourceresolver.LogicalSourceRecord;
import com.taxonic.carml.model.TriplesMap;
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
