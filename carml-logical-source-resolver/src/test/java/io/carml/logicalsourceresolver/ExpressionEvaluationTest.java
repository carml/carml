package io.carml.logicalsourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.junit.jupiter.api.Test;

class ExpressionEvaluationTest {

  @Test
  void givenEvaluationResult_whenExtractValues_thenReturnExpectedListOfStrings() {
    // Given
    List<Integer> evaluationResult = List.of(1, 2, 3);

    // When
    List<String> extractedValues = ExpressionEvaluation.extractStringValues(evaluationResult);

    // Then
    assertThat(extractedValues, is(List.of("1", "2", "3")));
  }

  @Test
  void givenNullEvaluationResult_whenExtractValues_thenReturnEmptyList() {
    // Given
    Object evaluationResult = null;

    // When
    List<String> extractedValues = ExpressionEvaluation.extractStringValues(evaluationResult);

    // Then
    assertThat(extractedValues, is(List.of()));
  }

  @Test
  void givenSingularEvaluationResult_whenExtractValues_thenReturnListWithSingleExpectedString() {
    // Given
    boolean evaluationResult = true;

    // When
    List<String> extractedValues = ExpressionEvaluation.extractStringValues(evaluationResult);

    // Then
    assertThat(extractedValues, is(List.of("true")));
  }

}
