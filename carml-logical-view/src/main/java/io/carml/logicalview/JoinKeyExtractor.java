package io.carml.logicalview;

import static java.util.function.Predicate.not;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ExpressionMap;
import io.carml.model.Join;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Builds positional join keys from {@link Join} conditions, evaluating each condition's parent or
 * child {@link ExpressionMap} against the supplied {@link ExpressionEvaluation} via an
 * {@link ExpressionMapEvaluator}. Keys normalize each value component to its
 * {@link Object#toString()} so cross-source comparisons work even when one side returns numeric and
 * the other returns string for the same logical key (e.g. SQL parents returning {@link Integer}
 * where the CSV child stores the same id as a {@link String}).
 *
 * <p>Used by {@link InMemoryJoinExecutor} and any spillable executor implementation in sibling
 * modules. Reused outside {@link DefaultLogicalViewEvaluator} so the evaluation logic lives in one
 * place.
 */
public final class JoinKeyExtractor {

    private JoinKeyExtractor() {}

    /**
     * Builds the parent-side join key from a {@link ViewIteration}. The iteration is wrapped in a
     * {@link ViewIterationExpressionEvaluation} so parent {@link ExpressionMap} references resolve
     * against the iteration's field values, with strict validation against the supplied
     * referenceable key set.
     *
     * @param conditions the join conditions in stable order
     * @param parent the parent iteration
     * @param parentReferenceableKeys the parent view's referenceable keys
     * @param evaluator the evaluator used to resolve each condition's parent expression map
     * @return a key as a list of values, or an empty list if any condition produced no value
     */
    public static List<Object> parentKey(
            List<Join> conditions,
            ViewIteration parent,
            Set<String> parentReferenceableKeys,
            ExpressionMapEvaluator evaluator) {
        var exprEval = new ViewIterationExpressionEvaluation(parent, parentReferenceableKeys);
        var datatypeMapper = iterationDatatypeMapper(parent);
        return evaluateJoinKey(conditions, exprEval, true, evaluator, datatypeMapper);
    }

    /**
     * Builds the child-side join key from an {@link EvaluatedValues}. The expression evaluation is
     * a simple lookup against the row's value map — child key references point to absolute field
     * names already present in the child evaluation.
     *
     * @param conditions the join conditions in stable order
     * @param child the child row
     * @param evaluator the evaluator used to resolve each condition's child expression map
     * @return a key as a list of values, or an empty list if any condition produced no value
     */
    public static List<Object> childKey(
            List<Join> conditions, EvaluatedValues child, ExpressionMapEvaluator evaluator) {
        ExpressionEvaluation childExprEval =
                expression -> Optional.ofNullable(child.values().get(expression));
        DatatypeMapper datatypeMapper =
                field -> Optional.ofNullable(child.naturalDatatypes().get(field));
        return evaluateJoinKey(conditions, childExprEval, false, evaluator, datatypeMapper);
    }

    /**
     * Evaluates a join key from the given conditions and side. Visible for use by spillable
     * executor implementations that build keys against alternate {@link ExpressionEvaluation}
     * sources.
     *
     * @param conditions the join conditions in stable order
     * @param exprEval the expression evaluation context to resolve references against
     * @param isParent {@code true} to evaluate the parent map of each condition, {@code false} for
     *     the child map
     * @param evaluator the evaluator used to resolve each condition's expression map
     * @param datatypeMapper the datatype mapper associated with the row, passed through for
     *     canonical lexical form resolution of typed references
     * @return a key as a list of values, or an empty list if any condition produced no value
     */
    public static List<Object> evaluateJoinKey(
            List<Join> conditions,
            ExpressionEvaluation exprEval,
            boolean isParent,
            ExpressionMapEvaluator evaluator,
            DatatypeMapper datatypeMapper) {
        // takeWhile short-circuits on the first empty condition value.
        // The post-stream size check distinguishes "stopped early" from
        // "all conditions evaluated" — only the latter produces a usable key. Values are normalized
        // to String for consistent cross-source comparison (SQL resolvers may return Integer/Long
        // while the child field stores String, breaking Integer.equals(String)).
        var key = conditions.stream()
                .map(join -> isParent ? join.getParentMap() : join.getChildMap())
                .map(expressionMap -> evaluator.evaluate(expressionMap, exprEval, datatypeMapper))
                .takeWhile(not(List::isEmpty))
                .map(values -> (Object) values.get(0).toString())
                .toList();
        return key.size() == conditions.size() ? key : List.of();
    }

    private static DatatypeMapper iterationDatatypeMapper(ViewIteration parent) {
        return parent::getNaturalDatatype;
    }
}
