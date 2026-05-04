package io.carml.functions;

import java.io.Serial;

/**
 * Signals that a registered function's Java implementation threw at invocation time — for example
 * a {@link NullPointerException} from passing NULL to a non-nullable parameter, an
 * {@link IllegalArgumentException} for malformed input, or a reflective access/permission error.
 *
 * <p>Sibling to {@link FunctionEvaluationException}, which signals mapping-level failures during
 * function-execution evaluation (unresolvable function IRI, parameter map producing no value,
 * {@code rml:returnMap} IRI not declared as an {@code fno:Output} of the descriptor). Both
 * propagate up through the surrounding mapping pipeline so the caller fails with a diagnosable
 * cause, matching the RML-FNML conformance suite's {@code hasError=true} expectations
 * (e.g. {@code RMLFNMLTC0101} for runtime failures, {@code RMLFNMLTC0102}/{@code 0104} for
 * unresolvable function / return references). The two types exist to distinguish "the mapping
 * referenced something the engine could not resolve" from "the function impl raised at runtime";
 * neither is silently swallowed.
 */
public final class FunctionInvocationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public FunctionInvocationException(String message, Throwable cause) {
        super(message, cause);
    }
}
