package io.carml.engine;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.List;
import java.util.stream.Stream;

/**
 * Specialization of {@link TermGenerator} that can emit values lazily via a {@link Stream}, rather
 * than materializing the entire result set as a {@link List} in heap.
 *
 * <p>Used by gather-map term generators with cartesian-product strategies: at large multivalue
 * scales the per-record output cardinality is the product of input gather-slot sizes and quickly
 * exceeds available heap when materialized eagerly. Downstream consumers detect this interface via
 * {@code instanceof} and route the per-object emissions through a backpressure-aware reactive
 * pipeline.
 *
 * <p>The default {@link #apply} implementation collects the stream into a list, preserving
 * compatibility with callers that have not been adapted to the streaming path yet.
 *
 * @param <T> the value type
 */
public interface StreamingTermGenerator<T> extends TermGenerator<T> {

    /**
     * Returns a lazy stream of mapped values for the current iteration. The returned stream is
     * single-use and may be backed by per-element computation; callers are expected to fully consume
     * (or close) it within the same iteration.
     *
     * @param expressionEvaluation the expression evaluation for the current iteration
     * @param datatypeMapper the datatype mapper for the current iteration
     * @return a lazy {@link Stream} of mapped values
     */
    Stream<MappedValue<T>> applyAsStream(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper);

    @Override
    default List<MappedValue<T>> apply(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        try (var stream = applyAsStream(expressionEvaluation, datatypeMapper)) {
            return stream.toList();
        }
    }
}
