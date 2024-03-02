package io.carml.model.impl;

import static java.util.stream.Collectors.joining;

import io.carml.model.Template;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.mutable.MutableInt;


@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public class CarmlTemplate implements Template {

  private final List<Segment> segments;

  private final Set<ReferenceExpression> referenceExpressions;

  private final Map<ExpressionSegment, ReferenceExpression> expressionSegmentMap;

  public static Template of(List<Segment> segments) {
    var expressionSegmentMap = createExpressionSegmentMap(segments);
    var expressions = new LinkedHashSet<>(expressionSegmentMap.values());

    return new CarmlTemplate(segments, expressions, expressionSegmentMap);
  }

  private static Map<ExpressionSegment, ReferenceExpression> createExpressionSegmentMap(List<Segment> segments) {
    MutableInt id = new MutableInt();
    return segments.stream()
        .filter(ExpressionSegment.class::isInstance)
        .map(ExpressionSegment.class::cast)
        .collect(
            Collectors.toUnmodifiableMap(e -> e, e -> CarmlReferenceExpression.of(id.getAndIncrement(), e.getValue())));
  }

  @Override
  public List<Segment> getSegments() {
    return segments;
  }

  @Override
  public Set<ReferenceExpression> getReferenceExpressions() {
    return referenceExpressions;
  }

  @Override
  public Map<ExpressionSegment, ReferenceExpression> getExpressionSegmentMap() {
    return expressionSegmentMap;
  }

  @Override
  public String toTemplateString() {
    return segments.stream()
        .map(Segment::getTemplateStringRepresentation)
        .collect(joining());
  }

  @Override
  public Template adaptExpressions(UnaryOperator<String> referenceExpressionAdapter) {
    var adaptedSegments = segments.stream()
        .map(segment -> adaptSegment(segment, referenceExpressionAdapter))
        .toList();

    return of(adaptedSegments);
  }

  private Segment adaptSegment(Segment segment, UnaryOperator<String> referenceExpressionAdapter) {
    if (segment instanceof ExpressionSegment expressionSegment) {
      var adaptedValue = referenceExpressionAdapter.apply(expressionSegment.getValue());
      return new ExpressionSegment(expressionSegment.id, adaptedValue);
    } else {
      return segment;
    }
  }

  public record TextSegment(String value) implements Segment {

    @Override
    public String getValue() {
      return value;
    }

    @Override
    public String getTemplateStringRepresentation() {
      return value;
    }
  }

  public record ExpressionSegment(int id, String value) implements Segment {

    @Override
    public String getValue() {
      return value;
    }

    @Override
    public String getTemplateStringRepresentation() {
      return String.format("{%s}", value);
    }
  }

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @EqualsAndHashCode
  @ToString
  public static class CarmlReferenceExpression implements Template.ReferenceExpression {

    private final int id;

    private final String value;

    public static CarmlReferenceExpression of(int id, String value) {
      return new CarmlReferenceExpression(id, value);
    }

    @Override
    public String getValue() {
      return value;
    }
  }
}
