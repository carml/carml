package io.carml.engine;

import com.google.common.collect.Sets;
import io.carml.model.Template;
import io.carml.model.Template.ReferenceExpression;
import io.carml.model.Template.Segment;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import io.carml.model.impl.template.TemplateException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TemplateEvaluation implements Supplier<Set<String>> {

  private final Template template;

  private final Map<ReferenceExpression, Function<ReferenceExpression, Optional<Object>>> bindings;

  public static TemplateEvaluationBuilder builder() {
    return new TemplateEvaluationBuilder();
  }

  @Override
  public Set<String> get() {
    var indexedExprValues = new HashMap<Segment, List<String>>();

    // single out expression segments
    var expressionSegments = template.getSegments()
        .stream()
        .filter(ExpressionSegment.class::isInstance)
        .map(ExpressionSegment.class::cast)
        .toList();

    if (!expressionSegments.isEmpty()) {

      // map segment to list of its evaluation results
      for (var expressionSegment : expressionSegments) {
        var evalResult = getExpressionSegmentValue(expressionSegment);
        indexedExprValues.put(expressionSegment, evalResult.map(this::getValuesExpressionEvaluation)
            .orElse(List.of()));
      }

      // if there is an expression that doesn't result in a value,
      // the template should yield no result, following the RML rules.
      if (!exprValueResultsHasOnlyFilledLists(indexedExprValues)) {
        return Set.of();
      }

      return processSegments(indexedExprValues);
    }

    // if there are no expression segments, continue building value
    var result = template.getSegments()
        .stream()
        .map(Segment::getValue)
        .collect(Collectors.joining());

    return Set.of(result);
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

  private Set<String> processSegments(Map<Segment, List<String>> indexedExprValues) {
    var processedSegments = template.getSegments()
        .stream()
        .map(segment -> processSegment(segment, indexedExprValues))
        .toList();

    return Sets.cartesianProduct(processedSegments)
        .stream()
        .map(segmentValues -> String.join("", segmentValues))
        .collect(Collectors.toUnmodifiableSet());
  }

  private Set<String> processSegment(Segment segment, Map<Segment, List<String>> indexedExprValues) {
    if (segment instanceof TextSegment) {
      return Set.of(segment.getValue());
    }

    return Set.copyOf(indexedExprValues.get(segment));
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
