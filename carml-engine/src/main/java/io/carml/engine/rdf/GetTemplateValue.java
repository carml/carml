package io.carml.engine.rdf;

import io.carml.engine.TemplateEvaluation;
import io.carml.engine.TemplateEvaluation.TemplateEvaluationBuilder;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.Template;
import io.carml.model.Template.ReferenceExpression;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetTemplateValue implements BiFunction<ExpressionEvaluation, DatatypeMapper, Optional<Object>> {

  private static final Logger LOG = LoggerFactory.getLogger(GetTemplateValue.class);

  private final Template template;

  private final Function<String, String> transformValue;

  private final BiFunction<Object, IRI, String> createNaturalRdfLexicalForm;

  public GetTemplateValue(Template template, UnaryOperator<String> transformValue,
      BiFunction<Object, IRI, String> createNaturalRdfLexicalForm) {
    this.template = template;
    this.transformValue = transformValue;
    this.createNaturalRdfLexicalForm = createNaturalRdfLexicalForm;
  }

  @Override
  public Optional<Object> apply(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Processing template: {}", template.toTemplateString());
    }
    var templateEvaluationBuilder = TemplateEvaluation.builder()
        .template(template);

    template.getReferenceExpressions()
        .forEach(expression -> bindTemplateExpression(expression, expressionEvaluation, datatypeMapper,
            templateEvaluationBuilder));

    var templateResult = templateEvaluationBuilder.build()
        .get();
    // TODO change signature of class.
    return templateResult.isEmpty() ? Optional.empty() : Optional.of(templateResult);
  }

  private void bindTemplateExpression(ReferenceExpression expression, ExpressionEvaluation expressionEvaluation,
      DatatypeMapper datatypeMapper, TemplateEvaluationBuilder templateEvaluatorBuilder) {
    var datatype = datatypeMapper != null ? datatypeMapper.apply(expression.getValue())
        .orElse(XSD.STRING) : XSD.STRING;
    templateEvaluatorBuilder.bind(expression, expr -> expressionEvaluation.apply(expr.getValue())
        .map(result -> prepareValueForTemplate(result, datatype)));
  }

  // See https://www.w3.org/TR/r2rml/#from-template
  private Object prepareValueForTemplate(Object result, IRI datatype) {
    Objects.requireNonNull(result);

    if (result instanceof Collection<?>) {
      return ((Collection<?>) result).stream()
          .filter(Objects::nonNull)
          .map(rawValue -> createNaturalRdfLexicalForm.apply(rawValue, datatype))
          .map(transformValue)
          .toList();
    } else {
      String value = createNaturalRdfLexicalForm.apply(result, datatype);
      return transformValue.apply(value);
    }
  }
}
