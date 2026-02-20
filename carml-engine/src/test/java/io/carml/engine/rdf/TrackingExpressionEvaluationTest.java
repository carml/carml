package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TrackingExpressionEvaluationTest {

    @Test
    void givenExpressionThatProducesResult_whenApply_thenTrackExpression() {
        // Given
        ExpressionEvaluation delegate = expression -> Optional.of(List.of("value"));
        Set<String> matchedExpressions = TrackingExpressionEvaluation.createMatchedExpressionsSet();
        var tracking = TrackingExpressionEvaluation.of(delegate, matchedExpressions);

        // When
        var result = tracking.apply("$.name");

        // Then
        assertThat(result.isPresent(), is(true));
        assertThat(matchedExpressions, contains("$.name"));
    }

    @Test
    void givenExpressionThatProducesEmpty_whenApply_thenDoNotTrackExpression() {
        // Given
        ExpressionEvaluation delegate = expression -> Optional.empty();
        Set<String> matchedExpressions = TrackingExpressionEvaluation.createMatchedExpressionsSet();
        var tracking = TrackingExpressionEvaluation.of(delegate, matchedExpressions);

        // When
        var result = tracking.apply("$.nonExistent");

        // Then
        assertThat(result.isPresent(), is(false));
        assertThat(matchedExpressions, is(empty()));
    }

    @Test
    void givenMultipleExpressions_whenApply_thenTrackOnlyMatchedOnes() {
        // Given
        ExpressionEvaluation delegate = expression -> {
            if ("$.name".equals(expression)) {
                return Optional.of(List.of("Venus"));
            }
            return Optional.empty();
        };
        Set<String> matchedExpressions = TrackingExpressionEvaluation.createMatchedExpressionsSet();
        var tracking = TrackingExpressionEvaluation.of(delegate, matchedExpressions);

        // When
        tracking.apply("$.name");
        tracking.apply("$.nonExistent");
        tracking.apply("$.alsoNonExistent");

        // Then
        assertThat(matchedExpressions.size(), is(1));
        assertThat(matchedExpressions, contains("$.name"));
    }

    @Test
    void givenExpressionThatProducesEmptyList_whenApply_thenTrackExpression() {
        // Given - Optional.of(emptyList) means the key structurally exists but holds no values
        ExpressionEvaluation delegate = expression -> Optional.of(List.of());
        Set<String> matchedExpressions = TrackingExpressionEvaluation.createMatchedExpressionsSet();
        var tracking = TrackingExpressionEvaluation.of(delegate, matchedExpressions);

        // When
        var result = tracking.apply("$.emptyArray");

        // Then
        assertThat(result.isPresent(), is(true));
        assertThat(matchedExpressions, contains("$.emptyArray"));
    }

    @Test
    void givenSharedMatchedSet_whenMultipleTrackersApply_thenAccumulateMatches() {
        // Given
        Set<String> matchedExpressions = TrackingExpressionEvaluation.createMatchedExpressionsSet();

        ExpressionEvaluation delegate1 = expression -> {
            if ("$.name".equals(expression)) {
                return Optional.of(List.of("Venus"));
            }
            return Optional.empty();
        };

        ExpressionEvaluation delegate2 = expression -> {
            if ("$.age".equals(expression)) {
                return Optional.of(List.of("25"));
            }
            return Optional.empty();
        };

        var tracker1 = TrackingExpressionEvaluation.of(delegate1, matchedExpressions);
        var tracker2 = TrackingExpressionEvaluation.of(delegate2, matchedExpressions);

        // When
        tracker1.apply("$.name");
        tracker2.apply("$.age");

        // Then
        assertThat(matchedExpressions.size(), is(2));
        assertThat(matchedExpressions.contains("$.name"), is(true));
        assertThat(matchedExpressions.contains("$.age"), is(true));
    }
}
