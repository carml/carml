package io.carml.engine;

import io.carml.model.Template;
import io.carml.model.Template.ReferenceExpression;
import io.carml.model.Template.Segment;
import io.carml.model.impl.CarmlTemplate;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import io.carml.model.impl.template.TemplateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TemplateEvaluation implements Supplier<Optional<Object>> {

  private final Template template;

  private final Map<ReferenceExpression, Function<ReferenceExpression, Optional<Object>>> bindings;

  public static TemplateEvaluationBuilder builder() {
    return new TemplateEvaluationBuilder();
  }

  @Override
  public Optional<Object> get() {
    var result = new ArrayList<String>();
    var indexedExprValues = new HashMap<Segment, List<String>>();

    // single out expression segments
    var expressionSegments = template.getSegments()
        .stream()
        .filter(ExpressionSegment.class::isInstance)
        .map(ExpressionSegment.class::cast)
        .toList();

    if (!expressionSegments.isEmpty()) {

      // map segment to list of its evaluation results
      for (ExpressionSegment expressionSegment : expressionSegments) {
        Optional<Object> evalResult = getExpressionSegmentValue(expressionSegment);
        indexedExprValues.put(expressionSegment, evalResult.map(this::getValuesExpressionEvaluation)
            .orElse(List.of()));
      }

      // if there is an expression that doesn't result in a value,
      // the template should yield no result, following the RML rules.
      if (!exprValueResultsHasOnlyFilledLists(indexedExprValues)) {
        return Optional.empty();
      }

      // make sure that, if there are multiple segments, that they lead
      // an equal amount of values. If this is not the case, we cannot
      // know which values belong together over multiple segments.
      int indexedExprValSize = checkExprValueResultsOfEqualSizeAndReturn(indexedExprValues);
      processFixedNumberOfSegments(indexedExprValSize, indexedExprValues, result);
      // if there are no expression segments, continue building value
    } else {
      StringBuilder str = new StringBuilder();
      template.getSegments()
          .forEach(segment -> str.append(segment.getValue()));
      result.add(str.toString());
    }

    return Optional.of(result);
  }

  private Optional<Object> getExpressionSegmentValue(ExpressionSegment segment) {
    var expression = template.getExpressionSegmentMap()
        .get(segment);
    if (expression == null) { // Should never occur
      throw new TemplateException(
          String.format("no reference expression instance present corresponding to segment %s", segment));
    }

    return getExpressionValue(expression);
  }

  private Optional<Object> getExpressionValue(ReferenceExpression expression) {
    if (!bindings.containsKey(expression)) {
      throw new TemplateException(String.format("no binding present for reference expression [%s]", expression));
    }

    return bindings.get(expression)
        .apply(expression);
  }

  private List<String> getValuesExpressionEvaluation(Object evalResult) {
    if (evalResult instanceof Collection<?>) {
      return ((Collection<?>) evalResult).stream()
          .map(String.class::cast)
          .toList();
    } else {
      return List.of((String) evalResult);
    }
  }

  private boolean exprValueResultsHasOnlyFilledLists(Map<Segment, List<String>> indexedExprValues) {
    for (List<String> list : indexedExprValues.values()) {
      if (list.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private int checkExprValueResultsOfEqualSizeAndReturn(Map<Segment, List<String>> indexedExprValues) {
    int size = -1;
    for (List<String> list : indexedExprValues.values()) {
      if (size == -1) {
        size = list.size();
      } else {
        if (list.size() != size) {
          throw new TemplateException(String.format("Template expressions do not lead to an equal amount of values: %s",
              indexedExprValues.keySet()));
        }
      }
    }
    return size;
  }

  private void processFixedNumberOfSegments(int nrOfSegments, Map<Segment, List<String>> indexedExprValues,
      List<String> result) {
    for (int i = 0; i < nrOfSegments; i++) {
      StringBuilder str = new StringBuilder();
      for (Segment segment : template.getSegments()) {
        if (segment instanceof TextSegment) {
          str.append(segment.getValue());
        } else if (segment instanceof CarmlTemplate.ExpressionSegment) {
          String exprValue = indexedExprValues.get(segment)
              .get(i);
          if (exprValue == null) {
            result.add(null);
            continue;
          }
          str.append(exprValue);
        }
      }
      result.add(str.toString());
    }
  }

  public static final class TemplateEvaluationBuilder {

    private Template template;

    private final Map<ReferenceExpression, Function<ReferenceExpression, Optional<Object>>> bindings =
        new LinkedHashMap<>();

    public TemplateEvaluationBuilder template(Template template) {
      this.template = template;
      return this;
    }

    public TemplateEvaluationBuilder bind(ReferenceExpression expression,
        Function<ReferenceExpression, Optional<Object>> templateValue) {
      bindings.put(expression, templateValue);
      return this;
    }

    public TemplateEvaluation build() {
      if (template == null) {
        throw new TemplateException("template is required");
      }
      checkBindings();

      return new TemplateEvaluation(template, bindings);
    }

    private void checkBindings() {
      if (!new LinkedHashSet<>(bindings.keySet()).equals(template.getReferenceExpressions())) {
        throw new TemplateException(
            String.format("set of bindings [%s] does NOT match set of reference expressions in template [%s]",
                bindings.keySet(), template.getReferenceExpressions()));
      }
    }
  }
}
