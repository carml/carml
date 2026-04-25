package io.carml.logicalview;

/**
 * Factory for {@link InMemoryJoinExecutor}. Each
 * {@link #create(ExpressionMapEvaluator) create} call returns a new executor instance with no
 * shared state.
 */
public final class InMemoryJoinExecutorFactory implements JoinExecutorFactory {

    @Override
    public JoinExecutor create(ExpressionMapEvaluator evaluator) {
        return new InMemoryJoinExecutor(evaluator);
    }

    @Override
    public boolean cachesParentsInMemory() {
        return true;
    }
}
