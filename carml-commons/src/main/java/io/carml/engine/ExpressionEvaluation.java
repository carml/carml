package io.carml.engine;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface ExpressionEvaluation extends Function<String, Optional<Object>> {

  /**
   * Utility method to extract values from an executed {@link ExpressionEvaluation}. For example:
   *
   * <pre>
   * {@code
   *   expressionEvaluation.apply("foo")
   *       .map(ExpressionEvaluation::extractValues)
   *       .orElse(List.of());
   *}
   * </pre>
   *
   * @param evaluationResult The result of the {@link ExpressionEvaluation}
   * @return A {@link List} of {@link String} expression result values
   */
  static List<String> extractValues(Object evaluationResult) {
    if (evaluationResult instanceof Collection<?>) {
      return ((Collection<?>) evaluationResult).stream()
          .filter(Objects::nonNull)
          .map(Object::toString)
          .collect(Collectors.toUnmodifiableList());
    } else {
      return evaluationResult == null ? List.of() : List.of(evaluationResult.toString());
    }
  }

}
