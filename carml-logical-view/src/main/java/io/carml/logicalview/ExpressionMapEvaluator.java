package io.carml.logicalview;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ExpressionMap;
import java.util.List;

/**
 * SPI for evaluating an {@link ExpressionMap} against a row's {@link ExpressionEvaluation} and
 * {@link DatatypeMapper}, returning the (possibly multi-valued) result list. Used by the join
 * pipeline to resolve both child- and parent-side expression maps when building join keys, and by
 * the logical-view evaluator to resolve join-field and expression-field values.
 *
 * <p>The default implementation is {@link DefaultExpressionMapEvaluator}, backed by a
 * {@link io.carml.functions.FunctionRegistry}. It emits <b>raw values</b> suitable for
 * intermediate consumers (join-key equality comparison, expression-field storage for later
 * re-evaluation by downstream term generation).
 *
 * <p>This SPI is deliberately not used for RDF-term construction. {@code carml-engine}'s
 * {@code RdfExpressionMapEvaluation} sits at the <em>final</em> pipeline step and pre-shapes
 * values to be term-ready (typed {@link org.eclipse.rdf4j.model.Value} for Literals, IRI-safe
 * strings for IRIs, canonical lexical form for typed references). Applying those transforms at
 * the SPI level would be wrong for join keys (non-IRI fields) and would double-apply for
 * expression fields. See the two classes' Javadoc for details on how they divide responsibility.
 */
@FunctionalInterface
public interface ExpressionMapEvaluator {

    /**
     * Evaluates the given expression map against the supplied row context.
     *
     * @param expressionMap the expression map to evaluate
     * @param expressionEvaluation the row's expression evaluation function
     * @param datatypeMapper the row's datatype mapper, used to resolve natural datatypes for
     *     function-execution inputs; may be {@code null}
     * @return the evaluation results, or an empty list if the expression produces no values
     */
    List<Object> evaluate(
            ExpressionMap expressionMap, ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper);
}
