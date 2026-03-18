package io.carml.engine;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalSource;
import io.carml.model.TriplesMap;
import io.carml.output.NTriplesTermEncoder;
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

    /**
     * Maps a {@link ViewIteration} directly to N-Triples/N-Quads bytes, bypassing Statement object
     * creation for maximum throughput. Returns both the encoded bytes and any mergeable results that
     * need cross-iteration handling.
     *
     * @param viewIteration the view iteration to map
     * @param encoder the encoder to use for byte serialization
     * @param includeGraph whether to include the graph field in the encoded output (true for
     *     N-Quads, false for N-Triples)
     * @return a {@link ByteMappingResult} containing encoded bytes and mergeable results
     */
    default ByteMappingResult<V> mapToBytes(
            ViewIteration viewIteration, NTriplesTermEncoder encoder, boolean includeGraph) {
        throw new UnsupportedOperationException("Byte-mode mapping not supported by this implementation");
    }

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
}
