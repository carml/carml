package io.carml.logicalview;

/**
 * Factory for creating {@link JoinExecutor} instances. Each invocation of
 * {@link #create(ExpressionMapEvaluator)} must return a fresh, independent executor — the
 * evaluator constructs one executor per join and closes it via try/using when the join completes.
 *
 * <p>The default in-memory factory is exposed via {@link #inMemory()}. Spillable implementations
 * (e.g. DuckDB-backed) live in their own modules and are constructed explicitly by the
 * application; they are not auto-discovered.
 */
@FunctionalInterface
public interface JoinExecutorFactory {

    /**
     * Creates a fresh {@link JoinExecutor}. Each returned executor has its own state and lifecycle —
     * implementations must not share mutable state between returned instances. The executor uses the
     * supplied {@link ExpressionMapEvaluator} to resolve each join condition's child and parent
     * expression maps when building join keys; this allows function-valued {@code rml:childMap} /
     * {@code rml:parentMap} to participate in joins.
     *
     * @param evaluator the evaluator used to resolve condition expression maps into key values
     * @return a new join executor
     */
    JoinExecutor create(ExpressionMapEvaluator evaluator);

    /**
     * Returns {@code true} if the executors created by this factory materialize the parent stream
     * into the JVM heap during {@link JoinExecutor#matches}. The evaluator uses this to decide
     * whether it can also write the materialized list back into a per-mapping parent cache for
     * reuse across joins to the same parent view, without imposing additional materialization cost.
     *
     * <p>Default: {@code false} — safe for spillable executors that stream directly to disk.
     *
     * @return {@code true} if this factory's executors fully materialize parents in memory
     */
    default boolean cachesParentsInMemory() {
        return false;
    }

    /**
     * Returns the default in-memory factory backed by {@link InMemoryJoinExecutor}. Drains parents
     * to a HashMap probe table; same memory profile as the pre-Task-6.41 evaluator.
     *
     * @return an in-memory join executor factory
     */
    static JoinExecutorFactory inMemory() {
        return new InMemoryJoinExecutorFactory();
    }
}
