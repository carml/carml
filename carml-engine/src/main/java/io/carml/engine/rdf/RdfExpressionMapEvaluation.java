package io.carml.engine.rdf;

import static io.carml.engine.rdf.RdfPredicateObjectMapper.createObjectMapGenerators;
import static io.carml.engine.rdf.RdfPredicateObjectMapper.createPredicateGenerators;
import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductMappedStatements;
import static io.carml.util.LogUtil.exception;
import static org.eclipse.rdf4j.model.util.Values.bnode;

import io.carml.engine.MappedValue;
import io.carml.engine.TemplateEvaluation;
import io.carml.engine.TemplateEvaluation.TemplateEvaluationBuilder;
import io.carml.engine.function.BuiltInFunctionProvider;
import io.carml.engine.function.FunctionDescriptor;
import io.carml.engine.function.FunctionRegistry;
import io.carml.engine.function.ParameterDescriptor;
import io.carml.engine.function.TypeCoercer;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.Condition;
import io.carml.model.DatatypeMap;
import io.carml.model.ExpressionMap;
import io.carml.model.FunctionExecution;
import io.carml.model.Input;
import io.carml.model.LanguageMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.Template.ReferenceExpression;
import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf;
import java.text.Normalizer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.common.net.ParsedIRI;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.XSD;

@Slf4j
@Builder(toBuilder = true)
public class RdfExpressionMapEvaluation {

    private final ExpressionMap expressionMap;

    private final ExpressionEvaluation expressionEvaluation;

    private final DatatypeMapper datatypeMapper;

    private final RdfTermGeneratorFactory rdfTermGeneratorFactory;

    private final FunctionRegistry functionRegistry;

    private final Normalizer.Form normalizationForm;

    @Default
    private final UnaryOperator<String> templateReferenceValueTransformingFunction = UnaryOperator.identity();

    @Default
    private final Set<String> iriSafeFieldNames = Set.of();

    @Default
    private final BiFunction<Object, IRI, String> rdfLexicalForm = CanonicalRdfLexicalForm.get();

    @SuppressWarnings("unchecked")
    public <T> List<T> evaluate(Class<T> expectedType) {
        // Gate: evaluate conditions first — if any condition is falsy, produce no values
        var conditions = expressionMap.getConditions();
        if (!conditions.isEmpty() && !evaluateConditions(conditions)) {
            return List.of();
        }

        if (expressionMap.getConstant() != null) {
            if (expectedType == String.class) {
                return evaluateConstant().stream()
                        .map(Value::stringValue)
                        .map(expectedType::cast)
                        .toList();
            }
            return (List<T>) evaluateConstant();
        } else if (expressionMap.getReference() != null) {
            return (List<T>) evaluateReference();
        } else if (expressionMap.getTemplate() != null) {
            return (List<T>) evaluateTemplate();
        } else if (expressionMap.getFunctionExecution() != null) {
            return (List<T>) evaluateFnmlFunctionExecution();
        } else if (expressionMap.getFunctionValue() != null) {
            return (List<T>) evaluateFunctionValue();
        } else {
            throw new RdfExpressionMapEvaluationException(
                    "Encountered expressionMap without an expression %s".formatted(exception(expressionMap)));
        }
    }

    private List<Value> evaluateConstant() {
        return List.of(expressionMap.getConstant());
    }

    private List<Object> evaluateReference() {
        return expressionEvaluation
                .apply(expressionMap.getReference())
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private List<String> evaluateTemplate() {
        var template = expressionMap.getTemplate();

        var templateEvaluationBuilder = TemplateEvaluation.builder().template(template);

        template.getReferenceExpressions()
                .forEach(expression -> bindTemplateExpression(expression, templateEvaluationBuilder));

        return templateEvaluationBuilder.build().get();
    }

    private void bindTemplateExpression(
            ReferenceExpression expression, TemplateEvaluationBuilder templateEvaluatorBuilder) {
        var datatype = datatypeMapper == null
                ? XSD.STRING
                : datatypeMapper.apply(expression.getValue()).orElse(XSD.STRING);

        var effectiveTransform = iriSafeFieldNames.contains(expression.getValue())
                ? UnaryOperator.<String>identity()
                : templateReferenceValueTransformingFunction;

        templateEvaluatorBuilder.bind(expression, expr -> expressionEvaluation
                .apply(expr.getValue())
                .map(result -> prepareValueForTemplate(result, datatype, effectiveTransform))
                .orElse(List.of()));
    }

    private List<String> prepareValueForTemplate(Object result, IRI datatype, UnaryOperator<String> transform) {
        if (result instanceof Collection<?>) {
            return ((Collection<?>) result)
                    .stream()
                            .filter(Objects::nonNull)
                            .map(rawValue -> transformValueForTemplate(rawValue, datatype, transform))
                            .toList();
        } else {
            return List.of(transformValueForTemplate(result, datatype, transform));
        }
    }

    private String transformValueForTemplate(Object result, IRI datatype, UnaryOperator<String> transform) {
        return rdfLexicalForm.andThen(transform).apply(result, datatype);
    }

    private List<Object> evaluateFunctionValue() {
        var functionValue = expressionMap.getFunctionValue();

        return mapLegacyFunctionExecution(expressionMap, functionValue)
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private List<Object> evaluateFnmlFunctionExecution() {
        try {
            return doEvaluateFnmlFunctionExecution();
        } catch (RdfExpressionMapEvaluationException exception) {
            // Graceful degradation per RML-FNML spec: unknown function, unknown parameter,
            // or unknown return IRI produces empty output rather than an error.
            LOG.warn("Function execution produced no result: {}", exception.getMessage());
            return List.of();
        } catch (IllegalStateException exception) {
            // Graceful degradation: function invocation failures (e.g. null parameter due to
            // unknown parameter binding, type mismatch, NPE inside the function) produce
            // empty output. ReflectiveFunctionDescriptor wraps invocation errors in
            // IllegalStateException.
            LOG.warn("Function execution failed: {}", exception.getMessage());
            return List.of();
        }
    }

    private List<Object> doEvaluateFnmlFunctionExecution() {
        var fnExecution = expressionMap.getFunctionExecution();
        var termType = determineTermType(expressionMap);
        UnaryOperator<Object> returnValueAdapter =
                termType == TermType.IRI ? this::iriEncodeResult : UnaryOperator.identity();

        // 1. Resolve function IRI from FunctionMap
        var functionMap = fnExecution.getFunctionMap();
        if (functionMap == null) {
            throw new RdfExpressionMapEvaluationException(
                    "FunctionExecution has no FunctionMap (rml:functionMap/rml:function is required)");
        }
        IRI functionIri = resolveIriFromExpressionMap(functionMap, Rdf.Rml.FunctionMap);

        // 2. Look up descriptor
        FunctionDescriptor descriptor = lookupFunction(functionIri);

        // 3. Resolve input bindings
        Map<IRI, Object> parameterValues = resolveInputBindings(fnExecution.getInputs(), descriptor);
        LOG.debug(
                "Function {}: parameterValues = {}, descriptor paramIris = {}",
                functionIri,
                parameterValues,
                descriptor.getParameters().stream()
                        .map(ParameterDescriptor::parameterIri)
                        .toList());

        // 4. Execute
        Object result = descriptor.execute(parameterValues);
        if (result == null) {
            return List.of();
        }

        // 5. Apply ReturnMap if present
        result = applyReturnMap(result, descriptor);
        if (result == null) {
            return List.of();
        }

        // 6. Apply IRI encoding for IRI term types
        result = returnValueAdapter.apply(result);

        // 7. Extract values
        return ExpressionEvaluation.extractValues(result);
    }

    private FunctionDescriptor lookupFunction(IRI functionIri) {
        return functionRegistry
                .getFunction(functionIri)
                .orElseThrow(() -> new RdfExpressionMapEvaluationException(
                        "no function registered for function IRI [%s]".formatted(functionIri)));
    }

    private IRI resolveIriFromExpressionMap(ExpressionMap iriExpressionMap, IRI mapType) {
        var childEvaluation = this.toBuilder().expressionMap(iriExpressionMap).build();

        var values = childEvaluation.evaluate(Value.class);
        if (values.isEmpty()) {
            throw new RdfExpressionMapEvaluationException("%s did not produce a value".formatted(mapType));
        }

        var value = values.get(0);
        if (value instanceof IRI iri) {
            return iri;
        }
        throw new RdfExpressionMapEvaluationException("%s produced non-IRI value: %s".formatted(mapType, value));
    }

    private Map<IRI, Object> resolveInputBindings(Set<Input> inputs, FunctionDescriptor descriptor) {
        var parameterValues = new HashMap<IRI, Object>();

        for (var input : inputs) {
            // Resolve parameter IRI from ParameterMap
            IRI parameterIri = resolveIriFromExpressionMap(input.getParameterMap(), Rdf.Rml.ParameterMap);
            LOG.debug("Resolved parameter IRI: {}", parameterIri);

            // Resolve value from InputValueMap using a child evaluation
            var valueEvaluation =
                    this.toBuilder().expressionMap(input.getInputValueMap()).build();
            var values = valueEvaluation.evaluate(Object.class);
            LOG.debug("Resolved values for parameter {}: {}", parameterIri, values);

            // Find matching parameter descriptor for type coercion.
            // Match by both fno:predicate IRI and parameter resource IRI (e.g., rml:parameter
            // may reference the fno:Parameter resource IRI rather than the fno:predicate value).
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
                        effectiveKey, values.stream().map(this::toStringValue).toList());
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

    private String toStringValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Value rdfValue) {
            return rdfValue.stringValue();
        }
        return value.toString();
    }

    private Object applyReturnMap(Object result, FunctionDescriptor descriptor) {
        var returnMap = expressionMap.getReturnMap();
        if (returnMap == null) {
            return result;
        }

        IRI returnIri = resolveIriFromExpressionMap(returnMap, Rdf.Rml.ReturnMap);

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
        if (descriptor.getReturns().stream().anyMatch(rd -> rd.matches(returnIri))) {
            return result;
        }

        LOG.warn("ReturnMap IRI {} is not a known FnO output; producing empty result", returnIri);
        return null;
    }

    private Optional<Object> mapLegacyFunctionExecution(ExpressionMap expressionMap, TriplesMap executionMap) {
        MappedValue<Resource> functionExecution = RdfMappedValue.of(bnode());

        var executionStatements = executionMap.getPredicateObjectMaps().stream()
                .flatMap(pom ->
                        getFunctionPredicateObjectMapModel(functionExecution, executionMap, pom, expressionEvaluation))
                .collect(ModelCollector.toModel());

        var termType = determineTermType(expressionMap);

        // for IRI term types, make values valid IRIs.
        UnaryOperator<Object> returnValueAdapter = termType == TermType.IRI ? this::iriEncodeResult : v -> v;

        return mapExecution(executionStatements, returnValueAdapter);
    }

    private Stream<Statement> getFunctionPredicateObjectMapModel(
            MappedValue<Resource> functionExecution,
            TriplesMap executionMap,
            PredicateObjectMap pom,
            ExpressionEvaluation expressionEvaluation) {
        var predicateGenerators = createPredicateGenerators(pom, executionMap, rdfTermGeneratorFactory);
        var objectGenerators = createObjectMapGenerators(pom.getObjectMaps(), executionMap, rdfTermGeneratorFactory);

        Set<MappedValue<IRI>> predicates = predicateGenerators.stream()
                .map(g -> g.apply(expressionEvaluation, datatypeMapper))
                .flatMap(List::stream)
                .collect(Collectors.toUnmodifiableSet());

        if (predicates.isEmpty()) {
            return Stream.empty();
        }

        List<MappedValue<? extends Value>> objects = objectGenerators
                .map(g -> g.apply(expressionEvaluation, datatypeMapper))
                .<MappedValue<? extends Value>>flatMap(List::stream)
                .toList();

        if (objects.isEmpty()) {
            return Stream.empty();
        }

        return streamCartesianProductMappedStatements(Set.of(functionExecution), predicates, objects, Set.of())
                .map(MappedStatement.class::cast)
                .map(MappedStatement::getStatement);
    }

    private Optional<Object> mapExecution(Model executionStatements, UnaryOperator<Object> returnValueAdapter) {
        Optional<Resource> optionalExecution = Models.subject(executionStatements);

        return optionalExecution.map(execution -> {
            IRI functionIri = getFunctionIri(execution, executionStatements);
            FunctionDescriptor descriptor = lookupFunction(functionIri);

            var parameterValues = extractParameters(executionStatements, execution, descriptor);

            Object result = descriptor.execute(parameterValues);
            if (result == null) {
                return null;
            }
            return returnValueAdapter.apply(result);
        });
    }

    private static final TypeCoercer TYPE_COERCER = TypeCoercer.defaults();

    private Map<IRI, Object> extractParameters(Model model, Resource execution, FunctionDescriptor descriptor) {
        var params = new HashMap<IRI, Object>();

        for (var paramDesc : descriptor.getParameters()) {
            var values = model.filter(execution, paramDesc.parameterIri(), null).stream()
                    .map(Statement::getObject)
                    .toList();

            if (values.isEmpty()) {
                params.put(paramDesc.parameterIri(), null);
                continue;
            }

            if (Collection.class.isAssignableFrom(paramDesc.type())) {
                params.put(
                        paramDesc.parameterIri(),
                        values.stream().map(Value::stringValue).toList());
            } else {
                params.put(
                        paramDesc.parameterIri(),
                        TYPE_COERCER.coerce(values.get(0).stringValue(), paramDesc.type()));
            }
        }

        return params;
    }

    private Object iriEncodeResult(Object result) {
        if (result instanceof Collection<?>) {
            return ((Collection<?>) result).stream().map(this::encodeAsIri).toList();
        } else {
            return encodeAsIri(result);
        }
    }

    private Object encodeAsIri(Object value) {
        String iriValue;

        if (value instanceof Value objectValue) {
            iriValue = objectValue.stringValue();
        } else {
            iriValue = value.toString();
        }

        // perform unicode normalization
        iriValue = Normalizer.normalize(iriValue, normalizationForm);

        return ParsedIRI.create(iriValue).toString();
    }

    private IRI getFunctionIri(Resource execution, Model model) {
        return Models.objectIRI(model.filter(execution, Rdf.Fno.executes, null))
                .orElseGet(() -> Models.objectIRI(model.filter(execution, Rdf.Fno.old_executes, null))
                        .orElseThrow(() -> new RdfExpressionMapEvaluationException(
                                "function execution does not have fno:executes value")));
    }

    private TermType determineTermType(ExpressionMap map) {
        if (map instanceof DatatypeMap) {
            return TermType.IRI;
        } else if (map instanceof LanguageMap) {
            return TermType.LITERAL;
        } else if (map instanceof TermMap termMap) {
            TermType termType = termMap.getTermType();
            if (termType != null) {
                return termType;
            }

            if (map instanceof ObjectMap objectMap
                    && (isReferenceTermMap(termMap)
                            || objectMap.getLanguageMap() != null
                            || objectMap.getDatatypeMap() != null)) {
                return TermType.LITERAL;
            }

            return TermType.IRI;
        } else {
            throw new IllegalStateException(String.format(
                    "Unknown expression map type %s for %s", map.getClass().getSimpleName(), map));
        }
    }

    private boolean isReferenceTermMap(TermMap map) {
        return map.getConstant() == null && map.getReference() != null;
    }

    // --- Condition evaluation ---

    private boolean evaluateConditions(Set<Condition> conditions) {
        for (var condition : conditions) {
            if (!evaluateCondition(condition)) {
                return false;
            }
        }
        return true;
    }

    private boolean evaluateCondition(Condition condition) {
        // Full form: FunctionExecution returning boolean
        if (condition.getFunctionExecution() != null) {
            return evaluateConditionFunctionExecution(condition.getFunctionExecution());
        }

        // Shortcut: isNull
        if (condition.getIsNull() != null) {
            return evaluateUnaryCondition(Rdf.IdlabFn.isNull, condition.getIsNull());
        }

        // Shortcut: isNotNull
        if (condition.getIsNotNull() != null) {
            return evaluateUnaryCondition(Rdf.IdlabFn.isNotNull, condition.getIsNotNull());
        }

        // Shortcut: equals
        var equalsRefs = condition.getEquals();
        if (equalsRefs != null && !equalsRefs.isEmpty()) {
            return evaluateBinaryCondition(Rdf.IdlabFn.equal, equalsRefs);
        }

        // Shortcut: notEquals
        var notEqualsRefs = condition.getNotEquals();
        if (notEqualsRefs != null && !notEqualsRefs.isEmpty()) {
            return evaluateBinaryCondition(Rdf.IdlabFn.notEqual, notEqualsRefs);
        }

        throw new RdfExpressionMapEvaluationException("Condition has no evaluable expression");
    }

    private boolean evaluateConditionFunctionExecution(FunctionExecution fnExec) {
        var functionMap = fnExec.getFunctionMap();
        if (functionMap == null) {
            throw new RdfExpressionMapEvaluationException("Condition FunctionExecution has no FunctionMap");
        }
        IRI functionIri = resolveIriFromExpressionMap(functionMap, Rdf.Rml.FunctionMap);

        FunctionDescriptor descriptor = lookupFunction(functionIri);

        Map<IRI, Object> parameterValues = resolveInputBindings(fnExec.getInputs(), descriptor);
        Object result = descriptor.execute(parameterValues);
        return BuiltInFunctionProvider.isTruthy(result);
    }

    private boolean evaluateUnaryCondition(IRI functionIri, String reference) {
        FunctionDescriptor descriptor = lookupFunction(functionIri);

        Object value = expressionEvaluation.apply(reference).orElse(null);
        Map<IRI, Object> params = new HashMap<>();
        params.put(Rdf.IdlabFn.str, value);

        Object result = descriptor.execute(params);
        return BuiltInFunctionProvider.isTruthy(result);
    }

    private boolean evaluateBinaryCondition(IRI functionIri, Set<String> references) {
        if (references.size() != 2) {
            throw new RdfExpressionMapEvaluationException(
                    "Binary condition (equals/notEquals) requires exactly 2 references, but got %d"
                            .formatted(references.size()));
        }

        FunctionDescriptor descriptor = lookupFunction(functionIri);

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
}
