package io.carml.engine.rdf;

import static io.carml.engine.rdf.RdfPredicateObjectMapper.createObjectMapGenerators;
import static io.carml.engine.rdf.RdfPredicateObjectMapper.createPredicateGenerators;
import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductMappedStatements;
import static io.carml.util.LogUtil.exception;
import static org.eclipse.rdf4j.model.util.Values.bnode;

import io.carml.engine.MappedValue;
import io.carml.functions.FunctionDescriptor;
import io.carml.functions.FunctionEvaluationException;
import io.carml.functions.FunctionExecutionSupport;
import io.carml.functions.FunctionRegistry;
import io.carml.functions.TypeCoercer;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.DatatypeMap;
import io.carml.model.ExpressionMap;
import io.carml.model.LanguageMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.Template.ReferenceExpression;
import io.carml.model.TermMap;
import io.carml.model.TermType;
import io.carml.model.TriplesMap;
import io.carml.model.impl.template.TemplateEvaluation;
import io.carml.model.impl.template.TemplateEvaluation.TemplateEvaluationBuilder;
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

/**
 * Evaluates an {@link ExpressionMap} against a row's {@link ExpressionEvaluation} and
 * {@link DatatypeMapper}. Instances hold the immutable evaluation configuration and can be reused
 * across rows — only the per-row {@code expressionEvaluation} and {@code datatypeMapper} are
 * supplied to each {@link #evaluate} call.
 *
 * <h2>Pipeline position: final RDF-term-construction step</h2>
 *
 * <p>Used by {@link RdfTermGeneratorFactory} to produce values that become RDF terms
 * (IRI / Literal / BNode) directly. Unlike
 * {@code io.carml.logicalview.DefaultExpressionMapEvaluator} — which sits one step before
 * term construction and emits raw values for join-key comparison or expression-field storage —
 * this evaluator <b>pre-shapes</b> its output so the downstream
 * {@code RdfTermGeneratorFactory.createEvaluatingGenerator} wrapping step can wrap values directly
 * into typed RDF terms:
 *
 * <ul>
 *   <li><b>Constants</b> → raw {@link Value} objects. For typed constant Literals
 *       ({@code rml:constant "42"^^xsd:integer}) the embedded datatype is preserved so the
 *       resulting Literal carries it.</li>
 *   <li><b>Templates</b> → per-reference-expression bindings that apply
 *       {@link #rdfLexicalForm} (canonical lexical form keyed on the reference's natural datatype)
 *       then {@link #templateReferenceValueTransformingFunction} (IRI-safe percent-encoding for
 *       IRI-typed term maps), with per-field bypass via {@link #iriSafeFieldNames} when the source
 *       has declared a reference already IRI-safe.</li>
 *   <li><b>Function execution</b> → result optionally wrapped by {@code iriEncodeResult} when the
 *       enclosing term map has {@link TermType#IRI}.</li>
 *   <li><b>Legacy {@code fnml:functionValue}</b> → full RDF-model-driven execution via
 *       {@link RdfTermGeneratorFactory} (this branch is unavailable to the default evaluator
 *       because term generation belongs to {@code carml-engine}, not {@code carml-logical-view}).</li>
 * </ul>
 *
 * <p>These transforms must happen <em>inside</em> the evaluator because template segment rendering
 * is per-reference-expression (IRI-safe bypass depends on which field a segment references), and
 * because constant Literal terms need their datatype preserved end-to-end. The sibling default
 * evaluator deliberately skips them — applying IRI-safe encoding to a join-key value would be
 * wrong (it's not an IRI), and applying lexical form at the intermediate step would be
 * double-applied when term generation reapplies it.
 *
 * <p>For child evaluations (e.g. resolving function IRIs or input bindings), use
 * {@link #withExpressionMap(ExpressionMap)} to create a derived instance that shares the same
 * configuration but evaluates a different expression map.
 */
@Slf4j
public class RdfExpressionMapEvaluation {

    private static final TypeCoercer TYPE_COERCER = TypeCoercer.defaults();

    private final ExpressionMap expressionMap;

    private final RdfTermGeneratorFactory rdfTermGeneratorFactory;

    private final FunctionRegistry functionRegistry;

    private final Normalizer.Form normalizationForm;

    private final UnaryOperator<String> templateReferenceValueTransformingFunction;

    private final Set<String> iriSafeFieldNames;

    private final BiFunction<Object, IRI, String> rdfLexicalForm;

    private RdfExpressionMapEvaluation(
            ExpressionMap expressionMap,
            RdfTermGeneratorFactory rdfTermGeneratorFactory,
            FunctionRegistry functionRegistry,
            Normalizer.Form normalizationForm,
            UnaryOperator<String> templateReferenceValueTransformingFunction,
            Set<String> iriSafeFieldNames,
            BiFunction<Object, IRI, String> rdfLexicalForm) {
        this.expressionMap = expressionMap;
        this.rdfTermGeneratorFactory = rdfTermGeneratorFactory;
        this.functionRegistry = functionRegistry;
        this.normalizationForm = normalizationForm;
        this.templateReferenceValueTransformingFunction = templateReferenceValueTransformingFunction;
        this.iriSafeFieldNames = iriSafeFieldNames;
        this.rdfLexicalForm = rdfLexicalForm;
    }

    /**
     * Creates a builder for constructing a reusable {@link RdfExpressionMapEvaluation} instance.
     */
    public static RdfExpressionMapEvaluationBuilder builder() {
        return new RdfExpressionMapEvaluationBuilder();
    }

    /**
     * Returns a derived instance that shares this instance's configuration but evaluates a different
     * expression map. Used for child evaluations such as resolving function IRIs, parameter values,
     * and return maps. Package-private to allow test access.
     */
    RdfExpressionMapEvaluation withExpressionMap(ExpressionMap childExpressionMap) {
        return new RdfExpressionMapEvaluation(
                childExpressionMap,
                rdfTermGeneratorFactory,
                functionRegistry,
                normalizationForm,
                templateReferenceValueTransformingFunction,
                iriSafeFieldNames,
                rdfLexicalForm);
    }

    /**
     * Evaluates this expression map against the given row context.
     *
     * @param expressionEvaluation the row's expression evaluation function
     * @param datatypeMapper the row's datatype mapper (may be {@code null})
     * @param expectedType the expected result type
     * @return the evaluation results, or an empty list if the expression produces no values
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> evaluate(
            ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper, Class<T> expectedType) {
        // Gate: evaluate conditions first — if any is falsy, produce no values.
        if (FunctionExecutionSupport.anyConditionFails(
                expressionMap.getConditions(),
                functionRegistry,
                recursiveEvaluator(),
                expressionEvaluation,
                datatypeMapper)) {
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
            return (List<T>) evaluateReference(expressionEvaluation);
        } else if (expressionMap.getTemplate() != null) {
            return (List<T>) evaluateTemplate(expressionEvaluation, datatypeMapper);
        } else if (expressionMap.getFunctionExecution() != null) {
            return (List<T>) evaluateFnmlFunctionExecution(expressionEvaluation, datatypeMapper);
        } else if (expressionMap.getFunctionValue() != null) {
            return (List<T>) evaluateFunctionValue(expressionEvaluation, datatypeMapper);
        } else {
            throw new RdfExpressionMapEvaluationException(
                    "Encountered expressionMap without an expression %s".formatted(exception(expressionMap)));
        }
    }

    private List<Value> evaluateConstant() {
        return List.of(expressionMap.getConstant());
    }

    private List<Object> evaluateReference(ExpressionEvaluation expressionEvaluation) {
        return expressionEvaluation
                .apply(expressionMap.getReference())
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private List<String> evaluateTemplate(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        var template = expressionMap.getTemplate();

        var templateEvaluationBuilder = TemplateEvaluation.builder().template(template);

        template.getReferenceExpressions()
                .forEach(expression -> bindTemplateExpression(
                        expression, templateEvaluationBuilder, expressionEvaluation, datatypeMapper));

        return templateEvaluationBuilder.build().get();
    }

    private void bindTemplateExpression(
            ReferenceExpression expression,
            TemplateEvaluationBuilder templateEvaluatorBuilder,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        var datatype = datatypeMapper == null
                ? XSD.STRING
                : datatypeMapper.apply(expression.getValue()).orElse(XSD.STRING);

        var effectiveTransform = iriSafeFieldNames.contains(expression.getValue())
                ? UnaryOperator.<String>identity()
                : templateReferenceValueTransformingFunction;

        templateEvaluatorBuilder.bind(
                expression,
                expr -> expressionEvaluation
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

    private List<Object> evaluateFunctionValue(
            ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        var functionValue = expressionMap.getFunctionValue();

        return mapLegacyFunctionExecution(expressionMap, functionValue, expressionEvaluation, datatypeMapper)
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private List<Object> evaluateFnmlFunctionExecution(
            ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        return doEvaluateFnmlFunctionExecution(expressionEvaluation, datatypeMapper);
    }

    private List<Object> doEvaluateFnmlFunctionExecution(
            ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
        // Term-type-driven adaptation is the only engine-specific bit of the FNML pipeline; the
        // rest (FunctionMap resolution → descriptor lookup → input bindings → execute →
        // ReturnMap → extractValues) lives in the shared helper.
        UnaryOperator<Object> returnValueAdapter =
                determineTermType(expressionMap) == TermType.IRI ? this::iriEncodeResult : UnaryOperator.identity();
        return FunctionExecutionSupport.executeFunctionExecution(
                expressionMap,
                expressionMap.getFunctionExecution(),
                functionRegistry,
                recursiveEvaluator(),
                expressionEvaluation,
                datatypeMapper,
                returnValueAdapter);
    }

    /**
     * Adapts {@link #withExpressionMap(ExpressionMap)} to the shared
     * {@link FunctionExecutionSupport.RecursiveEvaluator} SAM. Child evaluations run with
     * {@code Object.class} as the expected type; function-execution sub-maps (FunctionMap,
     * ParameterMap, ReturnMap, InputValueMap) never flow through the TermType branch so there
     * is no IRI-encoding concern at this level.
     */
    private FunctionExecutionSupport.RecursiveEvaluator recursiveEvaluator() {
        return (em, ee, dm) -> withExpressionMap(em).evaluate(ee, dm, Object.class);
    }

    private FunctionDescriptor lookupFunction(IRI functionIri) {
        return functionRegistry
                .getFunction(functionIri)
                .orElseThrow(() -> new FunctionEvaluationException(
                        "no function registered for function IRI [%s]".formatted(functionIri)));
    }

    private Optional<Object> mapLegacyFunctionExecution(
            ExpressionMap expressionMap,
            TriplesMap executionMap,
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
        MappedValue<Resource> functionExecution = RdfMappedValue.of(bnode());

        var executionStatements = executionMap.getPredicateObjectMaps().stream()
                .flatMap(pom -> getFunctionPredicateObjectMapModel(
                        functionExecution, executionMap, pom, expressionEvaluation, datatypeMapper))
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
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper) {
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
            // determineTermType is only invoked from evaluateFnmlFunctionExecution and
            // evaluateFunctionValue, both reached while evaluating a TermMap. Non-TermMap
            // ExpressionMaps (FunctionMap, ParameterMap, ReturnMap, ChildMap, ParentMap,
            // InputValueMap) only recurse through this class via the shared
            // FunctionExecutionSupport helpers, which evaluate child maps with Object.class
            // and never reach this branch. Any non-TermMap arriving here is a bug.
            throw new IllegalStateException(
                    "Cannot determine TermType for non-TermMap ExpressionMap: %s".formatted(exception(map)));
        }
    }

    private boolean isReferenceTermMap(TermMap map) {
        return map.getConstant() == null && map.getReference() != null;
    }

    /**
     * Builder for constructing {@link RdfExpressionMapEvaluation} instances.
     */
    public static class RdfExpressionMapEvaluationBuilder {

        private ExpressionMap expressionMap;

        private RdfTermGeneratorFactory rdfTermGeneratorFactory;

        private FunctionRegistry functionRegistry;

        private Normalizer.Form normalizationForm;

        private UnaryOperator<String> templateReferenceValueTransformingFunction = UnaryOperator.identity();

        private Set<String> iriSafeFieldNames = Set.of();

        private BiFunction<Object, IRI, String> rdfLexicalForm = CanonicalRdfLexicalForm.get();

        RdfExpressionMapEvaluationBuilder() {}

        public RdfExpressionMapEvaluationBuilder expressionMap(ExpressionMap expressionMap) {
            this.expressionMap = expressionMap;
            return this;
        }

        public RdfExpressionMapEvaluationBuilder rdfTermGeneratorFactory(
                RdfTermGeneratorFactory rdfTermGeneratorFactory) {
            this.rdfTermGeneratorFactory = rdfTermGeneratorFactory;
            return this;
        }

        public RdfExpressionMapEvaluationBuilder functionRegistry(FunctionRegistry functionRegistry) {
            this.functionRegistry = functionRegistry;
            return this;
        }

        public RdfExpressionMapEvaluationBuilder normalizationForm(Normalizer.Form normalizationForm) {
            this.normalizationForm = normalizationForm;
            return this;
        }

        public RdfExpressionMapEvaluationBuilder templateReferenceValueTransformingFunction(
                UnaryOperator<String> templateReferenceValueTransformingFunction) {
            this.templateReferenceValueTransformingFunction = templateReferenceValueTransformingFunction;
            return this;
        }

        public RdfExpressionMapEvaluationBuilder iriSafeFieldNames(Set<String> iriSafeFieldNames) {
            this.iriSafeFieldNames = iriSafeFieldNames;
            return this;
        }

        public RdfExpressionMapEvaluationBuilder rdfLexicalForm(BiFunction<Object, IRI, String> rdfLexicalForm) {
            this.rdfLexicalForm = rdfLexicalForm;
            return this;
        }

        public RdfExpressionMapEvaluation build() {
            Objects.requireNonNull(expressionMap, "expressionMap must not be null");
            return new RdfExpressionMapEvaluation(
                    expressionMap,
                    rdfTermGeneratorFactory,
                    functionRegistry,
                    normalizationForm,
                    templateReferenceValueTransformingFunction,
                    iriSafeFieldNames,
                    rdfLexicalForm);
        }
    }
}
