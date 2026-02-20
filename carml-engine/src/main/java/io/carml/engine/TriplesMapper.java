package io.carml.engine;

import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.model.LogicalSource;
import io.carml.model.TriplesMap;
import org.eclipse.rdf4j.model.Resource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TriplesMapper<V> {

    Flux<MappingResult<V>> map(LogicalSourceRecord<?> logicalSourceRecord);

    Flux<MappingResult<V>> mapEvaluation(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper);

    TriplesMap getTriplesMap();

    LogicalSource getLogicalSource();

    ParentSideJoinConditionStore<MappedValue<Resource>> getParentSideJoinConditions();

    /**
     * Validates the mapping results after all records have been processed. In strict mode, this
     * checks whether all reference expressions produced at least one non-null result. Returns an
     * error signal if validation fails.
     *
     * @return a {@link Mono} that completes empty on success, or errors on validation failure
     */
    default Mono<Void> validate() {
        return Mono.empty();
    }

    void cleanup();
}
