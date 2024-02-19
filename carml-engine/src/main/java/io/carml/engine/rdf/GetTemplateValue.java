package io.carml.engine.rdf;

import io.carml.engine.template.Template;
import io.carml.engine.template.Template.Expression;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

  private final Set<Expression> expressions;

  private final Function<String, String> transformValue;

  private final BiFunction<Object, IRI, String> createNaturalRdfLexicalForm;

  public GetTemplateValue(Template template, Set<Expression> expressions, UnaryOperator<String> transformValue,
      BiFunction<Object, IRI, String> createNaturalRdfLexicalForm) {
    this.template = template;
    this.expressions = expressions;
    this.transformValue = transformValue;
    this.createNaturalRdfLexicalForm = createNaturalRdfLexicalForm;
  }

  @Override
  public Optional<Object> apply(ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Processing template: {}", template.toTemplateString());
    }
    Template.Builder templateBuilder = template.newBuilder();
    expressions.forEach(e -> bindTemplateExpression(e, expressionEvaluation, datatypeMapper, templateBuilder));
    return templateBuilder.create();
  }

  private void bindTemplateExpression(Expression expression, ExpressionEvaluation expressionEvaluation,
      DatatypeMapper datatypeMapper, Template.Builder templateBuilder) {
    var datatype = datatypeMapper != null ? datatypeMapper.apply(expression.getValue())
        .orElse(XSD.STRING) : XSD.STRING;
    templateBuilder.bind(expression, expr -> expressionEvaluation.apply(expr.getValue())
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
