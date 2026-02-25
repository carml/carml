package io.carml.engine;

import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalSource;
import io.carml.model.TriplesMap;
import org.eclipse.rdf4j.model.Resource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TriplesMapper<V> {

    Flux<MappingResult<V>> map(LogicalSourceRecord<?> logicalSourceRecord);

    /**
     * Maps a {@link ViewIteration} to mapping results. This entry point is used by the
     * LogicalView-based pipeline where iterations are resolved externally and passed directly to the
     * mapper.
     *
     * @param viewIteration the view iteration to map
     * @return a {@link Flux} of mapping results
     */
    default Flux<MappingResult<V>> map(ViewIteration viewIteration) {
        return Flux.error(
                new UnsupportedOperationException("ViewIteration mapping not supported by this implementation"));
    }

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
