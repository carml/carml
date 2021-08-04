package com.taxonic.carml.engine;

import com.taxonic.carml.engine.template.Template;
import com.taxonic.carml.engine.template.Template.Expression;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetTemplateValue implements Function<ExpressionEvaluation, Optional<Object>> {

  private static final Logger LOG = LoggerFactory.getLogger(GetTemplateValue.class);

  private final Template template;

  private final Set<Expression> expressions;

  private final Function<String, String> transformValue;

  private final Function<Object, String> createNaturalRdfLexicalForm;

  public GetTemplateValue(Template template, Set<Expression> expressions, UnaryOperator<String> transformValue,
      Function<Object, String> createNaturalRdfLexicalForm) {
    this.template = template;
    this.expressions = expressions;
    this.transformValue = transformValue;
    this.createNaturalRdfLexicalForm = createNaturalRdfLexicalForm;
  }

  @Override
  public Optional<Object> apply(ExpressionEvaluation expressionEvaluation) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Processing template: {}", template.toTemplateString());
    }
    Template.Builder templateBuilder = template.newBuilder();
    expressions.forEach(e -> bindTemplateExpression(e, expressionEvaluation, templateBuilder));
    return templateBuilder.create();
  }

  private void bindTemplateExpression(Expression expression, ExpressionEvaluation expressionEvaluation,
      Template.Builder templateBuilder) {
    templateBuilder.bind(expression, expr -> expressionEvaluation.apply(expr.getValue())
        .map(this::prepareValueForTemplate));
  }

  // See https://www.w3.org/TR/r2rml/#from-template
  private Object prepareValueForTemplate(Object raw) {
    Objects.requireNonNull(raw);

    if (raw instanceof Collection<?>) {
      return ((Collection<?>) raw).stream()
          .map(createNaturalRdfLexicalForm)
          .map(transformValue)
          .collect(Collectors.toUnmodifiableList());
    } else {
      String value = createNaturalRdfLexicalForm.apply(raw);
      return transformValue.apply(value);
    }
  }

}
