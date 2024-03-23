package io.carml.engine;

import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.model.LogicalSource;
import io.carml.model.TriplesMap;
import org.eclipse.rdf4j.model.Resource;
import reactor.core.publisher.Flux;

public interface TriplesMapper<V> {

    Flux<MappingResult<V>> map(LogicalSourceRecord<?> logicalSourceRecord);

    Flux<MappingResult<V>> mapEvaluation(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper);

    TriplesMap getTriplesMap();

    LogicalSource getLogicalSource();

    ParentSideJoinConditionStore<MappedValue<Resource>> getParentSideJoinConditions();

    void cleanup();
}
