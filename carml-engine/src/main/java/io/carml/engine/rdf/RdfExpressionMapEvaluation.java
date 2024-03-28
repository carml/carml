package io.carml.engine.rdf;

import static io.carml.engine.rdf.RdfPredicateObjectMapper.createObjectMapGenerators;
import static io.carml.engine.rdf.RdfPredicateObjectMapper.createPredicateGenerators;
import static io.carml.engine.rdf.util.MappedStatements.streamCartesianProductMappedStatements;
import static io.carml.util.LogUtil.exception;
import static org.eclipse.rdf4j.model.util.Values.bnode;

import io.carml.engine.MappedValue;
import io.carml.engine.TemplateEvaluation;
import io.carml.engine.TemplateEvaluation.TemplateEvaluationBuilder;
import io.carml.engine.function.ExecuteFunction;
import io.carml.engine.function.Functions;
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
import io.carml.vocab.Rdf;
import java.text.Normalizer;
import java.util.Collection;
import java.util.List;
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
@Builder
public class RdfExpressionMapEvaluation {

    private final ExpressionMap expressionMap;

    private final ExpressionEvaluation expressionEvaluation;

    private final DatatypeMapper datatypeMapper;

    private final RdfTermGeneratorFactory rdfTermGeneratorFactory;

    private final Functions functions;

    private final Normalizer.Form normalizationForm;

    @Default
    private final UnaryOperator<String> templateReferenceValueTransformingFunction = UnaryOperator.identity();

    @Default
    private final BiFunction<Object, IRI, String> rdfLexicalForm = CanonicalRdfLexicalForm.get();

    @SuppressWarnings("unchecked")
    public <T> List<T> evaluate(Class<T> expectedType) {
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
        } else if (expressionMap.getFunctionValue() != null) {
            return (List<T>) evaluateFunctionValue();
        } else {
            throw new RdfExpressionMapEvaluationException(
                    String.format("Encountered expressionMap without an expression %s", exception(expressionMap)));
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

        templateEvaluatorBuilder.bind(expression, expr -> expressionEvaluation
                .apply(expr.getValue())
                .map(result -> prepareValueForTemplate(result, datatype))
                .orElse(List.of()));
    }

    private List<String> prepareValueForTemplate(Object result, IRI datatype) {
        if (result instanceof Collection<?>) {
            return ((Collection<?>) result)
                    .stream()
                            .filter(Objects::nonNull)
                            .map(rawValue -> transformValueForTemplate(rawValue, datatype))
                            .toList();
        } else {
            return List.of(transformValueForTemplate(result, datatype));
        }
    }

    private String transformValueForTemplate(Object result, IRI datatype) {
        return rdfLexicalForm
                .andThen(templateReferenceValueTransformingFunction)
                .apply(result, datatype);
    }

    private List<Object> evaluateFunctionValue() {
        var functionValue = expressionMap.getFunctionValue();

        return mapFunctionExecution(expressionMap, functionValue)
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private Optional<Object> mapFunctionExecution(ExpressionMap expressionMap, TriplesMap executionMap) {
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

        Set<MappedValue<? extends Value>> objects = objectGenerators
                .map(g -> g.apply(expressionEvaluation, datatypeMapper))
                .flatMap(List::stream)
                .collect(Collectors.toUnmodifiableSet());

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
            ExecuteFunction function = functions
                    .getFunction(functionIri)
                    .orElseThrow(() -> new RdfExpressionMapEvaluationException(
                            "no function registered for function IRI [" + functionIri + "]"));

            return function.execute(executionStatements, execution, returnValueAdapter);
        });
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
}
