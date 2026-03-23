package io.carml.logicalview;

import static java.util.stream.Collectors.joining;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ReferenceFormulation;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;

/**
 * Bridges a {@link ViewIteration} to the {@link ExpressionEvaluation} interface, enabling
 * view-on-view evaluation where expressions are resolved as field-name lookups against a parent
 * view's iterations.
 *
 * <p>Validates that referenced expressions correspond to referenceable keys in the logical view.
 * Non-referenceable keys (the root {@code <it>} key and iterable record keys) and non-existing keys
 * are rejected with a descriptive error message.
 */
public class ViewIterationExpressionEvaluation implements ExpressionEvaluation {

    static final String IT_KEY = "<it>";

    private final ViewIteration iteration;

    private final Set<String> referenceableKeys;

    public ViewIterationExpressionEvaluation(ViewIteration iteration, Set<String> referenceableKeys) {
        this.iteration = iteration;
        this.referenceableKeys = referenceableKeys;
    }

    @Override
    public Optional<Object> apply(String expression) {
        if (!referenceableKeys.contains(expression)) {
            throw new ViewIterationExpressionEvaluationException(buildErrorMessage(expression));
        }
        return iteration.getValue(expression);
    }

    Optional<ReferenceFormulation> getFieldReferenceFormulation(String key) {
        return iteration.getFieldReferenceFormulation(key);
    }

    Optional<IRI> getNaturalDatatype(String key) {
        return iteration.getNaturalDatatype(key);
    }

    private String buildErrorMessage(String expression) {
        if (IT_KEY.equals(expression)) {
            return ("Reference to root iterable record key '%s' is not allowed;"
                            + " '%s' is not a referenceable key in a logical view")
                    .formatted(IT_KEY, IT_KEY);
        }

        if (referenceableKeys.contains(expression + DefaultLogicalViewEvaluator.INDEX_KEY_SUFFIX)) {
            return ("Reference to iterable record key '%s' is not allowed;"
                            + " '%s' is not a referenceable key."
                            + " Use its nested field names instead (e.g., '%s.fieldName')")
                    .formatted(expression, expression, expression);
        }

        var sortedKeys = referenceableKeys.stream().sorted().collect(joining(", "));
        return "Reference to non-existing key '%s'; the key does not exist in the logical view. Available keys: [%s]"
                .formatted(expression, sortedKeys);
    }
}
