package io.carml.functions;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.Condition;
import io.carml.model.ExpressionMap;
import io.carml.model.FunctionExecution;
import io.carml.model.Input;
import io.carml.vocab.Rdf;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Shared {@code rml:functionExecution} plumbing used by both RDF term-generation and
 * expression-field / join-key evaluation paths. Resolves function IRIs from FunctionMaps,
 * looks up {@link FunctionDescriptor}s in a {@link FunctionRegistry}, recursively evaluates
 * parameter IRIs and input value maps, executes the function, and optionally applies a
 * {@code rml:returnMap} to pick a single output from a multi-return function. Also evaluates
 * {@code rml:condition} (unary {@code isNull}/{@code isNotNull}, binary {@code equals}/
 * {@code notEquals}, and full {@code FunctionExecution}-valued conditions).
 *
 * <p>Callers supply a {@link RecursiveEvaluator} that evaluates a child {@link ExpressionMap}
 * against the current row — this keeps the helper independent of any particular evaluator SPI
 * in consumer modules.
 */
@Slf4j
public final class FunctionExecutionSupport {

    private static final TypeCoercer TYPE_COERCER = TypeCoercer.defaults();

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private FunctionExecutionSupport() {}

    /**
     * Callback for recursively evaluating a child {@link ExpressionMap} against the current row.
     * Callers adapt their own evaluator to this SAM so this helper does not depend on any
     * particular evaluator SPI.
     */
    @FunctionalInterface
    public interface RecursiveEvaluator {

        /**
         * Evaluates the given expression map against the supplied row context.
         *
         * @param expressionMap the expression map to evaluate
         * @param expressionEvaluation the row's expression evaluation function
         * @param datatypeMapper the row's datatype mapper (may be {@code null})
         * @return the evaluation results, or an empty list if the expression produces no values
         */
        List<Object> evaluate(
                ExpressionMap expressionMap, ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper);
    }

    /**
     * Resolves an IRI-valued {@link ExpressionMap} (FunctionMap, ParameterMap or ReturnMap).
     * Lenient by design: if the evaluator returns an {@link IRI} it is used directly; if it
     * returns any other {@link Value} its string form is wrapped via {@code createIRI}; any
     * other raw object is coerced via {@link Object#toString()}. Malformed IRIs still fail at
     * the {@code createIRI} boundary so no silent corruption occurs.
     *
     * @throws FunctionEvaluationException if the expression map produces no value
     */
    public static IRI resolveIri(
            ExpressionMap iriExpressionMap,
            IRI mapType,
            RecursiveEvaluator evaluator,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        var values = evaluator.evaluate(iriExpressionMap, expressionEvaluation, datatypeMapper);
        if (values.isEmpty()) {
            throw new FunctionEvaluationException("%s did not produce a value".formatted(mapType));
        }

        var raw = values.get(0);
        if (raw instanceof IRI iri) {
            return iri;
        }
        if (raw instanceof Value rdfValue) {
            return VF.createIRI(rdfValue.stringValue());
        }
        return VF.createIRI(raw.toString());
    }

    /**
     * Resolves the input bindings of a {@link FunctionExecution} by recursively evaluating each
     * {@link Input}'s parameter IRI and input value map, coercing single-valued inputs to the
     * parameter descriptor's declared Java type where applicable. Unknown parameters fall
     * through with the raw string value (graceful degradation). All declared parameters that
     * have no binding in the inputs are filled with {@code null}.
     */
    public static Map<IRI, Object> resolveInputBindings(
            Set<Input> inputs,
            FunctionDescriptor descriptor,
            RecursiveEvaluator evaluator,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        var parameterValues = new HashMap<IRI, Object>();

        for (var input : inputs) {
            IRI parameterIri = resolveIri(
                    input.getParameterMap(), Rdf.Rml.ParameterMap, evaluator, expressionEvaluation, datatypeMapper);

            var values = evaluator.evaluate(input.getInputValueMap(), expressionEvaluation, datatypeMapper);

            var paramDesc = descriptor.getParameters().stream()
                    .filter(pd -> pd.matches(parameterIri))
                    .findFirst();

            // Use the descriptor's canonical parameter IRI as map key so that
            // FunctionDescriptor.execute() can find the value. Fall back to the mapping IRI
            // when no descriptor matches (unknown parameter -- graceful degradation).
            IRI effectiveKey = paramDesc.map(ParameterDescriptor::parameterIri).orElse(parameterIri);

            if (values.isEmpty()) {
                parameterValues.put(effectiveKey, null);
            } else if (paramDesc.isPresent()
                    && Collection.class.isAssignableFrom(paramDesc.get().type())) {
                // Collection parameter: pass all values as list of strings
                parameterValues.put(
                        effectiveKey,
                        values.stream()
                                .map(FunctionExecutionSupport::toStringValue)
                                .toList());
            } else {
                // Single value: coerce to expected type
                var stringValue = toStringValue(values.get(0));
                if (paramDesc.isPresent()) {
                    parameterValues.put(
                            effectiveKey,
                            TYPE_COERCER.coerce(stringValue, paramDesc.get().type()));
                } else {
                    parameterValues.put(effectiveKey, stringValue);
                }
            }
        }

        // Fill in nulls for any parameters not provided by inputs
        for (var paramDesc : descriptor.getParameters()) {
            parameterValues.putIfAbsent(paramDesc.parameterIri(), null);
        }

        return parameterValues;
    }

    /**
     * Applies the {@code rml:returnMap} (if present) of the given expression map to the
     * function result. For multi-return functions (result is a {@code Map<IRI, Object>}) the
     * requested return value is selected by IRI. For single-return functions the return IRI is
     * validated against the declared {@code fno:Output} resources; an unknown IRI yields a
     * {@code null} return (graceful degradation).
     *
     * @return the possibly reshaped result, or {@code null} if the return IRI cannot be
     *     resolved
     */
    public static Object applyReturnMap(
            ExpressionMap expressionMap,
            Object result,
            FunctionDescriptor descriptor,
            RecursiveEvaluator evaluator,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        var returnMap = expressionMap.getReturnMap();
        if (returnMap == null) {
            return result;
        }

        IRI returnIri = resolveIri(returnMap, Rdf.Rml.ReturnMap, evaluator, expressionEvaluation, datatypeMapper);

        if (result instanceof Map<?, ?> resultMap) {
            // Multi-return function: select the requested return value from the map
            var selected = resultMap.get(returnIri);
            if (selected == null) {
                LOG.warn("ReturnMap IRI {} not found in function result keys {}", returnIri, resultMap.keySet());
            }
            return selected;
        }

        // Single-return function: verify the return IRI matches a declared fno:Output
        // resource. An IRI not declared as an output produces empty output (graceful
        // degradation per RML-FNML spec).
        if (descriptor.getReturns().stream().anyMatch(returnDescriptor -> returnDescriptor.matches(returnIri))) {
            return result;
        }

        LOG.warn("ReturnMap IRI {} is not a known FnO output; producing empty result", returnIri);
        return null;
    }

    /**
     * Orchestrates the full {@code rml:functionExecution} pipeline: resolves the function IRI
     * from the {@link FunctionExecution#getFunctionMap FunctionMap}, looks up the
     * {@link FunctionDescriptor}, resolves input bindings, invokes the function, applies any
     * {@code rml:returnMap}, and finally passes the raw result through the caller-supplied
     * {@code returnValueAdapter} before extracting the emitted values.
     *
     * <p>The adapter is the single seam between the non-RDF caller (join-key / expression-field
     * evaluation uses {@link java.util.function.UnaryOperator#identity()}) and the RDF
     * term-generation caller (which passes an IRI-encoding adapter for term maps declared as
     * {@code rr:IRI}).
     *
     * @return the list of emitted values, or {@link java.util.Collections#emptyList} when the
     *     function returned {@code null} or the return-map selection yielded no value
     * @throws FunctionEvaluationException when the function cannot be resolved or input binding
     *     fails (graceful degradation is the caller's responsibility)
     */
    public static List<Object> executeFunctionExecution(
            ExpressionMap expressionMap,
            FunctionExecution fnExecution,
            FunctionRegistry functionRegistry,
            RecursiveEvaluator evaluator,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper,
            UnaryOperator<Object> returnValueAdapter) {
        var functionMap = fnExecution.getFunctionMap();
        if (functionMap == null) {
            throw new FunctionEvaluationException(
                    "FunctionExecution has no FunctionMap (rml:functionMap/rml:function is required)");
        }
        IRI functionIri = resolveIri(functionMap, Rdf.Rml.FunctionMap, evaluator, expressionEvaluation, datatypeMapper);

        FunctionDescriptor descriptor = functionRegistry
                .getFunction(functionIri)
                .orElseThrow(() -> new FunctionEvaluationException(
                        "no function registered for function IRI [%s]".formatted(functionIri)));

        Map<IRI, Object> parameterValues = resolveInputBindings(
                fnExecution.getInputs(), descriptor, evaluator, expressionEvaluation, datatypeMapper);

        Object result = descriptor.execute(parameterValues);
        if (result == null) {
            return List.of();
        }

        result = applyReturnMap(expressionMap, result, descriptor, evaluator, expressionEvaluation, datatypeMapper);
        if (result == null) {
            return List.of();
        }

        result = returnValueAdapter.apply(result);

        return ExpressionEvaluation.extractValues(result);
    }

    /**
     * Evaluates the full set of conditions, short-circuiting on the first falsy result. Returns
     * {@code true} if any condition evaluates to falsy — i.e. the expression map should produce
     * no value. A {@code null} or empty condition set returns {@code false} (no gate).
     */
    public static boolean anyConditionFails(
            Set<Condition> conditions,
            FunctionRegistry functionRegistry,
            RecursiveEvaluator evaluator,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        if (conditions == null || conditions.isEmpty()) {
            return false;
        }
        for (var condition : conditions) {
            if (!evaluateCondition(condition, functionRegistry, evaluator, expressionEvaluation, datatypeMapper)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Renders a value as a plain string suitable for function parameter binding. {@link Value}
     * instances are unwrapped to their lexical form; all other non-null values fall back to
     * {@link Object#toString()}.
     */
    public static String toStringValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Value rdfValue) {
            return rdfValue.stringValue();
        }
        return value.toString();
    }

    private static boolean evaluateCondition(
            Condition condition,
            FunctionRegistry functionRegistry,
            RecursiveEvaluator evaluator,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        // Full form: FunctionExecution returning boolean
        if (condition.getFunctionExecution() != null) {
            return evaluateConditionFunctionExecution(
                    condition.getFunctionExecution(),
                    functionRegistry,
                    evaluator,
                    expressionEvaluation,
                    datatypeMapper);
        }

        // Shortcut: isNull
        if (condition.getIsNull() != null) {
            return evaluateUnaryCondition(
                    Rdf.IdlabFn.isNull, condition.getIsNull(), functionRegistry, expressionEvaluation);
        }

        // Shortcut: isNotNull
        if (condition.getIsNotNull() != null) {
            return evaluateUnaryCondition(
                    Rdf.IdlabFn.isNotNull, condition.getIsNotNull(), functionRegistry, expressionEvaluation);
        }

        // Shortcut: equals
        var equalsRefs = condition.getEquals();
        if (equalsRefs != null && !equalsRefs.isEmpty()) {
            return evaluateBinaryCondition(Rdf.IdlabFn.equal, equalsRefs, functionRegistry, expressionEvaluation);
        }

        // Shortcut: notEquals
        var notEqualsRefs = condition.getNotEquals();
        if (notEqualsRefs != null && !notEqualsRefs.isEmpty()) {
            return evaluateBinaryCondition(Rdf.IdlabFn.notEqual, notEqualsRefs, functionRegistry, expressionEvaluation);
        }

        throw new FunctionEvaluationException("Condition has no evaluable expression");
    }

    private static boolean evaluateConditionFunctionExecution(
            FunctionExecution fnExec,
            FunctionRegistry functionRegistry,
            RecursiveEvaluator evaluator,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        var functionMap = fnExec.getFunctionMap();
        if (functionMap == null) {
            throw new FunctionEvaluationException("Condition FunctionExecution has no FunctionMap");
        }
        IRI functionIri = resolveIri(functionMap, Rdf.Rml.FunctionMap, evaluator, expressionEvaluation, datatypeMapper);

        FunctionDescriptor descriptor = lookupFunction(functionRegistry, functionIri);

        Map<IRI, Object> parameterValues =
                resolveInputBindings(fnExec.getInputs(), descriptor, evaluator, expressionEvaluation, datatypeMapper);
        Object result = descriptor.execute(parameterValues);
        return BuiltInFunctionProvider.isTruthy(result);
    }

    private static boolean evaluateUnaryCondition(
            IRI functionIri,
            String reference,
            FunctionRegistry functionRegistry,
            ExpressionEvaluation expressionEvaluation) {
        FunctionDescriptor descriptor = lookupFunction(functionRegistry, functionIri);

        Object value = expressionEvaluation.apply(reference).orElse(null);
        Map<IRI, Object> params = new HashMap<>();
        params.put(Rdf.IdlabFn.str, value);

        Object result = descriptor.execute(params);
        return BuiltInFunctionProvider.isTruthy(result);
    }

    private static boolean evaluateBinaryCondition(
            IRI functionIri,
            Set<String> references,
            FunctionRegistry functionRegistry,
            ExpressionEvaluation expressionEvaluation) {
        if (references.size() != 2) {
            throw new FunctionEvaluationException(
                    "Binary condition (equals/notEquals) requires exactly 2 references, but got %d"
                            .formatted(references.size()));
        }

        FunctionDescriptor descriptor = lookupFunction(functionRegistry, functionIri);

        var iter = references.iterator();
        String ref1 = iter.next();
        Object val1 = expressionEvaluation.apply(ref1).orElse(null);

        String ref2 = iter.next();
        Object val2 = expressionEvaluation.apply(ref2).orElse(null);

        Map<IRI, Object> params = new HashMap<>();
        params.put(Rdf.Grel.valueParam, val1);
        params.put(Rdf.Grel.valueParam2, val2);

        Object result = descriptor.execute(params);
        return BuiltInFunctionProvider.isTruthy(result);
    }

    private static FunctionDescriptor lookupFunction(FunctionRegistry functionRegistry, IRI functionIri) {
        return functionRegistry
                .getFunction(functionIri)
                .orElseThrow(() -> new FunctionEvaluationException(
                        "no function registered for function IRI [%s]".formatted(functionIri)));
    }
}
