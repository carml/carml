package io.carml.engine.rdf;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;

/**
 * A decorating {@link ExpressionEvaluation} that tracks which expression references have produced at
 * least one non-empty result. Used in strict mode to detect non-existent references.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
class TrackingExpressionEvaluation implements ExpressionEvaluation {

    @NonNull
    private final ExpressionEvaluation delegate;

    @NonNull
    private final Set<String> matchedExpressions;

    static TrackingExpressionEvaluation of(ExpressionEvaluation delegate, Set<String> matchedExpressions) {
        return new TrackingExpressionEvaluation(delegate, matchedExpressions);
    }

    /**
     * Evaluates the expression via the delegate and tracks it as matched if the result is present.
     * Note: an {@code Optional.of(emptyList)} from an empty JSON array still counts as a match,
     * since the reference structurally exists in the source — only truly absent references
     * (returning {@code Optional.empty()}) are flagged by strict mode.
     */
    @Override
    public Optional<Object> apply(String expression) {
        var result = delegate.apply(expression);
        if (result.isPresent()) {
            matchedExpressions.add(expression);
        }
        return result;
    }

    /**
     * Creates a thread-safe set suitable for tracking matched expressions across multiple records.
     *
     * @return a new concurrent set for tracking matched expressions
     */
    static Set<String> createMatchedExpressionsSet() {
        return ConcurrentHashMap.newKeySet();
    }
}
