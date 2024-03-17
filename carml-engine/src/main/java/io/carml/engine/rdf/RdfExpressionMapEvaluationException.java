package io.carml.engine.rdf;

import java.io.Serial;

public class RdfExpressionMapEvaluationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 2100128088197325084L;

    public RdfExpressionMapEvaluationException(String message) {
        super(message);
    }

    public RdfExpressionMapEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }
}
