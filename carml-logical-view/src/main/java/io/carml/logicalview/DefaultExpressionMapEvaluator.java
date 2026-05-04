package io.carml.logicalview;

import io.carml.functions.FunctionExecutionSupport;
import io.carml.functions.FunctionRegistry;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ExpressionMap;
import io.carml.model.FunctionExecution;
import io.carml.model.Template;
import io.carml.model.impl.template.TemplateEvaluation;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;

/**
 * Default {@link ExpressionMapEvaluator} that resolves constant, reference, template and
 * {@code rml:functionExecution} expression maps (including recursive {@code rml:functionMap},
 * {@code rml:parameterMap}, {@code rml:inputValueMap} and {@code rml:returnMap} sub-evaluations)
 * against a row's {@link ExpressionEvaluation} and {@link DatatypeMapper}.
 *
 * <h2>Pipeline position: intermediate value producer</h2>
 *
 * <p>This evaluator sits one step before RDF term construction. Its output is consumed either as a
 * <em>join key</em> (equality-compared via {@code toString()} by
 * {@link JoinKeyExtractor#evaluateJoinKey}) or as an <em>expression-field value</em> (stored raw
 * on a {@link ViewIteration} and later retrieved via {@link ViewIteration#getValue(String)} by a
 * downstream {@code RdfExpressionMapEvaluation} that applies its own transforms at
 * term-generation time). Because of that positioning, this class deliberately emits <b>raw</b>
 * values:
 *
 * <ul>
 *   <li><b>Constants</b> → {@link org.eclipse.rdf4j.model.Value#stringValue()}. No datatype wrapper
 *       is preserved — the join-key / expression-field consumers only need the string form.</li>
 *   <li><b>Templates</b> → raw {@code toString()} segment concatenation via
 *       {@link io.carml.model.impl.template.TemplateEvaluation} with a bare
 *       {@link ExpressionEvaluation#extractStringValues} binding. <b>No canonical lexical form</b>,
 *       <b>no IRI-safe encoding</b>. Both sides of any join use the same evaluator so equality
 *       still matches; expression-field downstream term generation applies its own IRI-safe and
 *       lexical-form transforms when it constructs the eventual RDF terms.</li>
 *   <li><b>References</b> → {@link ExpressionEvaluation#extractValues} as-is.</li>
 * </ul>
 *
 * <p>Contrast with {@code io.carml.engine.rdf.RdfExpressionMapEvaluation} — the evaluator used by
 * RDF term generation — which sits at the <em>final</em> pipeline step and must pre-shape values
 * to be term-ready (typed {@link org.eclipse.rdf4j.model.Value} for Literals, IRI-safe strings for
 * IRIs). Moving those transforms into this evaluator would be incorrect for join keys (applying
 * IRI-safe encoding to non-IRI fields) and double-applied for expression fields (the downstream
 * term generator applies them again).
 *
 * <p>Legacy {@code fnml:functionValue} is not supported here — it requires building an RDF model
 * of the function execution using RDF term generators, which lives in the engine module. Mappings
 * that carry {@code rml:functionValue} in join conditions or expression-field positions silently
 * produce no values (logged at WARN). Use {@code rml:functionExecution} instead.
 */
@Slf4j
public final class DefaultExpressionMapEvaluator implements ExpressionMapEvaluator {

    private final FunctionRegistry functionRegistry;

    public DefaultExpressionMapEvaluator(FunctionRegistry functionRegistry) {
        this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry must not be null");
    }

    @Override
    public List<Object> evaluate(
            ExpressionMap expressionMap, ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        if (FunctionExecutionSupport.anyConditionFails(
                expressionMap.getConditions(),
                functionRegistry,
                this::evaluate,
                expressionEvaluation,
                datatypeMapper)) {
            return List.of();
        }

        if (expressionMap.getFunctionExecution() != null) {
            return evaluateFunctionExecution(
                    expressionMap, expressionMap.getFunctionExecution(), expressionEvaluation, datatypeMapper);
        }
        if (expressionMap.getFunctionValue() != null) {
            // Legacy fnml:functionValue requires term-generator-driven RDF modeling that doesn't
            // belong in the join-key / expression-field evaluation surface. Log a warning and
            // produce no value so the surrounding pipeline degrades gracefully.
            LOG.warn(
                    "Legacy fnml:functionValue encountered in a non-term-generation context (join key or "
                            + "expression field); it is not supported here. Use rml:functionExecution instead. "
                            + "Expression map: {}",
                    expressionMap);
            return List.of();
        }
        if (expressionMap.getConstant() != null) {
            return List.of(expressionMap.getConstant().stringValue());
        }
        if (expressionMap.getReference() != null) {
            return evaluateReference(expressionMap.getReference(), expressionEvaluation);
        }
        if (expressionMap.getTemplate() != null) {
            return evaluateTemplate(expressionMap.getTemplate(), expressionEvaluation);
        }
        return List.of();
    }

    private List<Object> evaluateFunctionExecution(
            ExpressionMap expressionMap,
            FunctionExecution fnExecution,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        // Non-RDF context: no term-type-specific return-value adaptation. Pass identity so the
        // shared orchestrator returns raw values suitable for join keys / expression fields.
        // Mapping-level errors (FunctionEvaluationException for unresolvable function / parameter
        // / return-IRI references) and runtime function failures (FunctionInvocationException)
        // propagate up rather than being swallowed; they surface as the surrounding mapping's
        // error.
        return FunctionExecutionSupport.executeFunctionExecution(
                expressionMap,
                fnExecution,
                functionRegistry,
                this::evaluate,
                expressionEvaluation,
                datatypeMapper,
                UnaryOperator.identity());
    }

    private static List<Object> evaluateReference(String reference, ExpressionEvaluation expressionEvaluation) {
        return expressionEvaluation
                .apply(reference)
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private static List<Object> evaluateTemplate(Template template, ExpressionEvaluation expressionEvaluation) {
        // Raw binding: each reference expression maps to its row values as plain strings, no IRI-safe
        // transform and no canonical lexical form. Term generation applies its own transforms on
        // top when it constructs RDF terms from these values.
        var builder = TemplateEvaluation.builder().template(template);
        template.getReferenceExpressions()
                .forEach(expression -> builder.bind(
                        expression,
                        expr -> expressionEvaluation
                                .apply(expr.getValue())
                                .map(ExpressionEvaluation::extractStringValues)
                                .orElse(List.of())));

        return builder
                .build() //
                .get()
                .stream()
                .map(s -> (Object) s)
                .toList();
    }
}
