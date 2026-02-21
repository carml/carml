package io.carml.logicalview;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.Optional;
import lombok.AllArgsConstructor;

/**
 * Bridges a {@link ViewIteration} to the {@link ExpressionEvaluation} interface, allowing the
 * mapping engine to evaluate expressions against view iterations using the same abstraction it uses
 * for logical source records.
 *
 * <p>Delegates {@link #apply(String)} to {@link ViewIteration#getValue(String)}.
 */
@AllArgsConstructor(staticName = "of")
public class ViewIterationExpressionEvaluation implements ExpressionEvaluation {

    private final ViewIteration viewIteration;

    @Override
    public Optional<Object> apply(String expression) {
        return viewIteration.getValue(expression);
    }
}
