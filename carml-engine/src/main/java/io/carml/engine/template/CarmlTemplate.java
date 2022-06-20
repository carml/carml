package io.carml.engine.template;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.mutable.MutableInt;

@EqualsAndHashCode
class CarmlTemplate implements Template {

  abstract static class Segment {

    private final String value;

    Segment(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  @EqualsAndHashCode(callSuper = false)
  static class Text extends Segment {

    Text(String value) {
      super(value);
    }

    @Override
    public String toString() {
      return "Text [getValue()=" + getValue() + "]";
    }
  }

  @EqualsAndHashCode(callSuper = false)
  static class ExpressionSegment extends Segment {

    private final int id;

    ExpressionSegment(int id, String value) {
      super(value);
      this.id = id;
    }

    @Override
    public String toString() {
      return "ExpressionSegment [getValue()=" + getValue() + "]";
    }
  }

  @EqualsAndHashCode
  private static class ExpressionImpl implements Expression {

    private final int id;

    String value;

    ExpressionImpl(int id, String value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "ExpressionImpl [id=" + id + ", value=" + value + "]";
    }
  }

  private class Builder implements Template.Builder {

    private final Map<Expression, Function<Expression, Optional<Object>>> bindings = new LinkedHashMap<>();

    @Override
    public Template.Builder bind(Expression expression, Function<Expression, Optional<Object>> templateValue) {
      bindings.put(expression, templateValue);
      return this;
    }

    private Optional<Object> getExpressionValue(Expression expression) {
      if (!bindings.containsKey(expression)) {
        throw new TemplateException(String.format("no binding present for expression [%s]", expression));
      }

      return bindings.get(expression)
          .apply(expression);
    }

    private Optional<Object> getExpressionSegmentValue(ExpressionSegment segment) {
      Expression expression = expressionSegmentMap.get(segment);
      if (expression == null) { // Should never occur
        throw new TemplateException(
            String.format("no Expression instance present corresponding to segment %s", segment));
      }

      return getExpressionValue(expression);
    }

    private void checkBindings() {
      if (!new LinkedHashSet<>(bindings.keySet()).equals(expressions)) {
        throw new TemplateException(String.format(
            "set of bindings [%s] does NOT match set of expressions in template [%s]", bindings.keySet(), expressions));
      }
    }

    private List<String> getValuesExpressionEvaluation(Object evalResult) {
      if (evalResult instanceof Collection<?>) {
        return ((Collection<?>) evalResult).stream()
            .map(String.class::cast)
            .collect(Collectors.toUnmodifiableList());
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
            throw new TemplateException(String.format(
                "Template expressions do not lead to an equal amount of values: %s", indexedExprValues.keySet()));
          }
        }
      }
      return size;
    }

    @Override
    public Optional<Object> create() {
      checkBindings();
      List<String> result = new ArrayList<>();
      Map<Segment, List<String>> indexedExprValues = new HashMap<>();

      // single out expression segments
      List<ExpressionSegment> expressionSegments = segments.stream()
          .filter(ExpressionSegment.class::isInstance)
          .map(ExpressionSegment.class::cast)
          .collect(Collectors.toUnmodifiableList());

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
        segments.forEach(segment -> str.append(segment.getValue()));
        result.add(str.toString());
      }

      return Optional.of(result);
    }

    private void processFixedNumberOfSegments(int nrOfSegments, Map<Segment, List<String>> indexedExprValues,
        List<String> result) {
      for (int i = 0; i < nrOfSegments; i++) {
        StringBuilder str = new StringBuilder();
        for (Segment s : segments) {
          if (s instanceof Text) {
            str.append(s.getValue());
          } else if (s instanceof ExpressionSegment) {
            String exprValue = indexedExprValues.get(s)
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
  }

  private static Map<ExpressionSegment, Expression> createExpressionSegmentMap(List<Segment> segments) {
    MutableInt id = new MutableInt();
    return segments.stream()
        .filter(ExpressionSegment.class::isInstance)
        .map(ExpressionSegment.class::cast)
        .collect(Collectors.toUnmodifiableMap(e -> e, e -> new ExpressionImpl(id.getAndIncrement(), e.getValue())));
  }

  static CarmlTemplate build(List<Segment> segments) {
    Map<ExpressionSegment, Expression> expressionSegmentMap = createExpressionSegmentMap(segments);
    Set<Expression> expressions = new LinkedHashSet<>(expressionSegmentMap.values());
    return new CarmlTemplate(segments, expressions, expressionSegmentMap);
  }

  private final List<Segment> segments;

  private final Set<Expression> expressions;

  private final Map<ExpressionSegment, Expression> expressionSegmentMap;

  CarmlTemplate(List<Segment> segments, Set<Expression> expressions,
      Map<ExpressionSegment, Expression> expressionSegmentMap) {
    this.segments = segments;
    this.expressions = expressions;
    this.expressionSegmentMap = expressionSegmentMap;
  }

  @Override
  public Set<Expression> getExpressions() {
    return expressions;
  }

  @Override
  public Template.Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "CarmlTemplate [segments=" + segments + ", expressions=" + expressions + ", expressionSegmentMap="
        + expressionSegmentMap + "]";
  }

  @Override
  public String toTemplateString() {
    return segments.stream()
        .map(s -> {
          if (s instanceof ExpressionSegment) {
            return String.format("{%s}", s.getValue());
          }
          return s.getValue();
        })
        .collect(Collectors.joining());
  }

}
