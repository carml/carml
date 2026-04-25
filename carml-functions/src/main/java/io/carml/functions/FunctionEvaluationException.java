package io.carml.functions;

import java.io.Serial;

/**
 * Signals that a function-execution path produced no usable result. Callers catch this to
 * gracefully degrade (log WARN and return an empty result) per the RML-FNML spec — unknown
 * function IRIs, missing FunctionMaps, non-matching ReturnMap IRIs, and similar recoverable
 * failures are reported through this exception.
 */
public final class FunctionEvaluationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public FunctionEvaluationException(String message) {
        super(message);
    }

    public FunctionEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }
}
