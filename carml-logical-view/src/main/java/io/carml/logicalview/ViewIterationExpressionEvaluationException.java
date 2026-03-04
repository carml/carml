package io.carml.logicalview;

import java.io.Serial;

/**
 * Thrown when a {@link ViewIterationExpressionEvaluation} encounters a reference to a
 * non-referenceable or non-existing key in a logical view iteration.
 */
public class ViewIterationExpressionEvaluationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -3827195041693025482L;

    ViewIterationExpressionEvaluationException(String message) {
        super(message);
    }
}
