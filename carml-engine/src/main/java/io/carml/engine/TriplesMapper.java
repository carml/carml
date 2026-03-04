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
     * When strict mode is enabled, checks that every reference expression in this TriplesMap produced
     * at least one non-null result across all processed records. Returns an error signal with a
     * {@link NonExistentReferenceException} if any expression never matched.
     *
     * <p>This check is only meaningful for the LogicalSource record-based pipeline. The LogicalView
     * pipeline does not need it because {@code ViewIterationExpressionEvaluation} validates field
     * existence eagerly during evaluation — a missing field fails immediately rather than silently
     * returning empty.
     *
     * @return a {@link Mono} that completes empty if all expressions matched (or strict mode is
     *     off), or errors if unmatched expressions are found
     */
    default Mono<Void> checkStrictModeExpressions() {
        return Mono.empty();
    }

    void cleanup();
}
