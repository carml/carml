package io.carml.model;

import java.util.Set;

/**
 * An RML condition that gates term generation. A condition evaluates to a boolean: if false, the
 * enclosing ExpressionMap produces no value.
 *
 * <p>Supports two forms:
 *
 * <ul>
 *   <li>Full form: {@link #getFunctionExecution()} returns a boolean-returning FunctionExecution
 *   <li>Shortcut form: one of {@link #getIsNull()}, {@link #getIsNotNull()}, {@link #getEquals()},
 *       {@link #getNotEquals()} is set
 * </ul>
 */
public interface Condition extends Resource {

    /** Full form: a FunctionExecution evaluating to boolean. */
    default FunctionExecution getFunctionExecution() {
        return null;
    }

    /** Shortcut: check if the referenced expression evaluates to null. */
    default String getIsNull() {
        return null;
    }

    /** Shortcut: check if the referenced expression evaluates to non-null. */
    default String getIsNotNull() {
        return null;
    }

    /**
     * Shortcut: check if two referenced expressions evaluate to equal values. Symmetric — order
     * doesn't matter.
     */
    default Set<String> getEquals() {
        return Set.of();
    }

    /**
     * Shortcut: check if two referenced expressions evaluate to non-equal values. Symmetric — order
     * doesn't matter.
     */
    default Set<String> getNotEquals() {
        return Set.of();
    }
}
