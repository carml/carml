package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableList;
import com.taxonic.carml.engine.template.Template;
import com.taxonic.carml.engine.template.Template.Expression;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetTemplateValue implements Function<ExpressionEvaluation, Optional<Object>> {

  private static final Logger LOG = LoggerFactory.getLogger(GetTemplateValue.class);

  private Template template;

  private Set<Expression> expressions;

  private Function<String, String> transformValue;

  private Function<Object, String> createNaturalRdfLexicalForm;

  public GetTemplateValue(Template template, Set<Expression> expressions, Function<String, String> transformValue,
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

  private Template.Builder bindTemplateExpression(Expression expression, ExpressionEvaluation expressionEvaluation,
      Template.Builder templateBuilder) {
    return templateBuilder.bind(expression, e -> expressionEvaluation.apply(e.getValue())
        .map(this::prepareValueForTemplate));
  }

  /**
   * See https://www.w3.org/TR/r2rml/#from-template
   *
   * @param raw
   * @return
   */
  private Object prepareValueForTemplate(Object raw) {
    Objects.requireNonNull(raw);

    if (raw instanceof Collection<?>) {
      return ((Collection<?>) raw).stream()
          .map(createNaturalRdfLexicalForm::apply)
          .map(transformValue::apply)
          .collect(ImmutableList.toImmutableList());
    } else {
      String value = createNaturalRdfLexicalForm.apply(raw);
      return transformValue.apply(value);
    }
  }

}
