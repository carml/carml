package io.carml.logicalview;

import io.carml.model.Join;
import java.util.List;
import java.util.Set;
import reactor.core.publisher.Flux;

/**
 * Executes a single join between a parent stream of {@link ViewIteration}s and a child stream of
 * {@link EvaluatedValues}, returning matched (child, parents) pairs preserving child stream order.
 *
 * <p>Implementations are responsible for releasing any resources they hold ({@link AutoCloseable})
 * — the caller wraps the returned {@link Flux} with {@link Flux#using(java.util.concurrent.Callable,
 * java.util.function.Function, java.util.function.Consumer)} to guarantee cleanup. {@link #close()}
 * must be idempotent.
 *
 * <p>For LEFT joins, children with no matching parents are still emitted with an empty
 * {@code matchedParents} list. For INNER joins, children with no matches are filtered out.
 *
 * <p>Implementations are not required to be thread-safe — each {@link JoinExecutor} instance is
 * obtained via {@link JoinExecutorFactory#create()} and used by a single join invocation.
 */
public interface JoinExecutor extends AutoCloseable {

    /**
     * Drains the parent and child streams, performs the join, and emits one {@link MatchedRow} per
     * child (or filters it out, for INNER joins with no match).
     *
     * @param parents the parent {@link ViewIteration} stream
     * @param children the child {@link EvaluatedValues} stream
     * @param conditions the join conditions, in a stable order (the executor uses positional
     *     evaluation against this order to build keys)
     * @param parentReferenceableKeys the set of parent keys that are valid references (used by the
     *     {@link ViewIterationExpressionEvaluation} when evaluating parent join keys)
     * @param leftJoin whether this is a LEFT join (child rows with no match are still emitted) or
     *     an INNER join (child rows with no match are filtered out)
     * @return a flux of matched rows preserving child stream order
     */
    Flux<MatchedRow> matches(
            Flux<ViewIteration> parents,
            Flux<EvaluatedValues> children,
            List<Join> conditions,
            Set<String> parentReferenceableKeys,
            boolean leftJoin);

    @Override
    default void close() {
        // No-op by default; overrides should release any held resources idempotently.
    }
}
