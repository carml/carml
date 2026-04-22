package io.carml.logicalview;

import static java.util.function.Predicate.not;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ExpressionMap;
import io.carml.model.Join;
import io.carml.model.Template;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import io.carml.util.CartesianProduct;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Builds positional join keys from {@link Join} conditions, evaluating each condition's parent or
 * child {@link ExpressionMap} against the supplied {@link ExpressionEvaluation}. Keys normalize
 * each value component to its {@link Object#toString()} so cross-source comparisons work even when
 * one side returns numeric and the other returns string for the same logical key (e.g. SQL parents
 * returning {@link Integer} where the CSV child stores the same id as a {@link String}).
 *
 * <p>Used by {@link InMemoryJoinExecutor} and any spillable executor implementation in sibling
 * modules. Reused outside {@link DefaultLogicalViewEvaluator} so the evaluation logic lives in one
 * place.
 *
 * <p><b>Limitation:</b> only constant, reference, and template expressions are supported in join
 * condition maps. {@code rml:functionExecution} throws {@link UnsupportedOperationException} and
 * {@code rml:functionValue} is ignored. The full evaluator with function support lives in
 * {@code carml-engine}'s {@code RdfExpressionMapEvaluation}; this evaluator and that one will be
 * unified behind a shared SPI in a future change.
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
     * @return a key as a list of String values, or an empty list if any condition produced no value
     */
    public static List<Object> parentKey(
            List<Join> conditions, ViewIteration parent, Set<String> parentReferenceableKeys) {
        var exprEval = new ViewIterationExpressionEvaluation(parent, parentReferenceableKeys);
        return evaluateJoinKey(conditions, exprEval, true);
    }

    /**
     * Builds the child-side join key from an {@link EvaluatedValues}. The expression evaluation is
     * a simple lookup against the row's value map — child key references point to absolute field
     * names already present in the child evaluation.
     *
     * @param conditions the join conditions in stable order
     * @param child the child row
     * @return a key as a list of String values, or an empty list if any condition produced no value
     */
    public static List<Object> childKey(List<Join> conditions, EvaluatedValues child) {
        ExpressionEvaluation childExprEval =
                expression -> Optional.ofNullable(child.values().get(expression));
        return evaluateJoinKey(conditions, childExprEval, false);
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
     * @return a key as a list of String values, or an empty list if any condition produced no value
     */
    public static List<Object> evaluateJoinKey(List<Join> conditions, ExpressionEvaluation exprEval, boolean isParent) {
        // takeWhile short-circuits on the first empty condition value.
        // The post-stream size check distinguishes "stopped early" from
        // "all conditions evaluated" — only the latter produces a usable key. Values are normalized
        // to String for consistent cross-source comparison (SQL resolvers may return Integer/Long
        // while the child field stores String, breaking Integer.equals(String)).
        var key = conditions.stream()
                .map(c -> isParent ? c.getParentMap() : c.getChildMap())
                .map(em -> evaluateExpressionMap(em, exprEval))
                .takeWhile(not(List::isEmpty))
                .map(values -> (Object) values.get(0).toString())
                .toList();
        return key.size() == conditions.size() ? key : List.of();
    }

    /**
     * Evaluates an arbitrary {@link ExpressionMap} (reference, template, or constant) against the
     * given {@link ExpressionEvaluation}, returning the (possibly multi-valued) result list.
     * Function executions are not supported and throw.
     */
    public static List<Object> evaluateExpressionMap(ExpressionMap field, ExpressionEvaluation exprEval) {
        if (field.getReference() != null) {
            return evaluateReference(field.getReference(), exprEval);
        }
        if (field.getTemplate() != null) {
            return evaluateTemplate(field.getTemplate(), exprEval);
        }
        if (field.getConstant() != null) {
            return List.of(field.getConstant().stringValue());
        }
        if (field.getFunctionExecution() != null) {
            throw new UnsupportedOperationException("Function execution in expression fields not yet supported");
        }
        return List.of();
    }

    private static List<Object> evaluateReference(String reference, ExpressionEvaluation exprEval) {
        return exprEval.apply(reference)
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private static List<Object> evaluateTemplate(Template template, ExpressionEvaluation exprEval) {
        var segmentValueLists = new ArrayList<List<String>>();

        for (var segment : template.getSegments()) {
            if (segment instanceof TextSegment textSegment) {
                segmentValueLists.add(List.of(textSegment.getValue()));
            } else if (segment instanceof ExpressionSegment expressionSegment) {
                var values = exprEval.apply(expressionSegment.getValue())
                        .map(ExpressionEvaluation::extractStringValues)
                        .orElse(List.of());

                if (values.isEmpty()) {
                    return List.of();
                }

                segmentValueLists.add(values);
            } else {
                throw new UnsupportedOperationException("Unsupported template segment type: %s"
                        .formatted(segment.getClass().getSimpleName()));
            }
        }

        var segmentCombinations = CartesianProduct.listCartesianProduct(segmentValueLists);

        return segmentCombinations.stream()
                .map(combination -> (Object) String.join("", combination))
                .toList();
    }
}
