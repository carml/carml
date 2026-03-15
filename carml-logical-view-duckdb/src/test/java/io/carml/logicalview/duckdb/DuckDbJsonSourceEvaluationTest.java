package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DuckDbJsonSourceEvaluationTest {

    @Nested
    class SingleValueExpressions {

        @Test
        void apply_withScalarReference_returnsSingleValue() {
            var eval = new DuckDbJsonSourceEvaluation("{\"name\": \"Alice\"}");

            var result = eval.apply("$.name");

            assertThat(result, is(Optional.of("Alice")));
        }

        @Test
        void apply_withNumericValue_returnsStringRepresentation() {
            var eval = new DuckDbJsonSourceEvaluation("{\"age\": 30}");

            var result = eval.apply("$.age");

            assertThat(result, is(Optional.of("30")));
        }

        @Test
        void apply_withBooleanValue_returnsStringRepresentation() {
            var eval = new DuckDbJsonSourceEvaluation("{\"active\": true}");

            var result = eval.apply("$.active");

            assertThat(result, is(Optional.of("true")));
        }

        @Test
        void apply_withNullValue_returnsEmpty() {
            var eval = new DuckDbJsonSourceEvaluation("{\"name\": null}");

            var result = eval.apply("$.name");

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void apply_withMissingPath_returnsEmpty() {
            var eval = new DuckDbJsonSourceEvaluation("{\"name\": \"Alice\"}");

            var result = eval.apply("$.missing");

            assertThat(result, is(Optional.empty()));
        }
    }

    @Nested
    class MultiValueExpressions {

        @Test
        void apply_withArrayWildcard_returnsListOfValues() {
            var eval = new DuckDbJsonSourceEvaluation("{\"values\": [\"a\", \"b\", \"c\"]}");

            var result = eval.apply("$.values[*]");

            assertThat(result, is(Optional.of(List.of("a", "b", "c"))));
        }

        @Test
        void apply_withChildWildcard_returnsListOfValues() {
            var eval = new DuckDbJsonSourceEvaluation("{\"values\": [\"1\", \"2\", \"3\"]}");

            var result = eval.apply("$.values.*");

            assertThat(result, is(Optional.of(List.of("1", "2", "3"))));
        }

        @Test
        void apply_withEmptyArray_returnsEmpty() {
            var eval = new DuckDbJsonSourceEvaluation("{\"values\": []}");

            var result = eval.apply("$.values[*]");

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void apply_withArrayOfNulls_returnsEmpty() {
            var eval = new DuckDbJsonSourceEvaluation("{\"values\": [null, null]}");

            var result = eval.apply("$.values[*]");

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void apply_withMixedNullsAndValues_filtersNulls() {
            var eval = new DuckDbJsonSourceEvaluation("{\"values\": [\"a\", null, \"c\"]}");

            var result = eval.apply("$.values[*]");

            assertThat(result, is(Optional.of(List.of("a", "c"))));
        }

        @Test
        void apply_withNumericArray_returnsStringRepresentations() {
            var eval = new DuckDbJsonSourceEvaluation("{\"values\": [1, 2, 3]}");

            var result = eval.apply("$.values[*]");

            assertThat(result, is(Optional.of(List.of("1", "2", "3"))));
        }
    }

    @Nested
    class NestedStructures {

        @Test
        void apply_withNestedObjectArray_skipsComplexValues() {
            var eval = new DuckDbJsonSourceEvaluation("{\"items\": [{\"name\": \"a\"}, {\"name\": \"b\"}]}");

            var result = eval.apply("$.items[*]");

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void apply_withNestedPath_returnsLeafValues() {
            var eval = new DuckDbJsonSourceEvaluation("{\"items\": [{\"name\": \"a\"}, {\"name\": \"b\"}]}");

            var result = eval.apply("$.items[*].name");

            assertThat(result, is(Optional.of(List.of("a", "b"))));
        }
    }

    @Nested
    class ReuseAcrossMultipleEvaluations {

        @Test
        void apply_calledMultipleTimes_returnCorrectResults() {
            var eval = new DuckDbJsonSourceEvaluation("{\"id\": \"x\", \"values\": [\"1\", \"2\"]}");

            assertThat(eval.apply("$.id"), is(Optional.of("x")));
            assertThat(eval.apply("$.values[*]"), is(Optional.of(List.of("1", "2"))));
            assertThat(eval.apply("$.missing"), is(Optional.empty()));
        }
    }
}
