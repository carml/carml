package io.carml.logicalview;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ReferenceFormulation;
import java.util.Optional;

/**
 * Bridges a {@link ViewIteration} to the {@link ExpressionEvaluation} interface, enabling
 * view-on-view evaluation where expressions are resolved as field-name lookups against a parent
 * view's iterations.
 */
class ViewIterationExpressionEvaluation implements ExpressionEvaluation {

    private final ViewIteration iteration;

    ViewIterationExpressionEvaluation(ViewIteration iteration) {
        this.iteration = iteration;
    }

    @Override
    public Optional<Object> apply(String expression) {
        return iteration.getValue(expression);
    }

    Optional<ReferenceFormulation> getFieldReferenceFormulation(String key) {
        return iteration.getFieldReferenceFormulation(key);
    }
}
