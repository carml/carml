package io.carml.engine.rdf;

import static io.carml.util.LogUtil.exception;

import io.carml.engine.TemplateEvaluation;
import io.carml.engine.TemplateEvaluation.TemplateEvaluationBuilder;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ExpressionMap;
import io.carml.model.Template.ReferenceExpression;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XSD;

@Slf4j
@Builder
public class RdfExpressionMapEvaluation {

    private final ExpressionMap expressionMap;

    private final ExpressionEvaluation expressionEvaluation;

    private final DatatypeMapper datatypeMapper;

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

    private List<String> evaluateFunctionValue() {
        return null;
    }
}
