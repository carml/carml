package io.carml.jsonpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JsonPathExpressionEvaluatorTest {

    @Nested
    class SingleValueExpressions {

        @Test
        void apply_withScalarReference_returnsSingleValue() {
            var evaluator = new JsonPathExpressionEvaluator("{\"name\": \"Alice\"}");

            var result = evaluator.apply("$.name");

            assertThat(result, is(Optional.of("Alice")));
        }

        @Test
        void apply_withNumericValue_returnsStringRepresentation() {
            var evaluator = new JsonPathExpressionEvaluator("{\"age\": 30}");

            var result = evaluator.apply("$.age");

            assertThat(result, is(Optional.of("30")));
        }

        @Test
        void apply_withBooleanValue_returnsStringRepresentation() {
            var evaluator = new JsonPathExpressionEvaluator("{\"active\": true}");

            var result = evaluator.apply("$.active");

            assertThat(result, is(Optional.of("true")));
        }

        @Test
        void apply_withNullValue_returnsEmpty() {
            var evaluator = new JsonPathExpressionEvaluator("{\"name\": null}");

            var result = evaluator.apply("$.name");

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void apply_withMissingPath_returnsEmpty() {
            var evaluator = new JsonPathExpressionEvaluator("{\"name\": \"Alice\"}");

            var result = evaluator.apply("$.missing");

            assertThat(result, is(Optional.empty()));
        }
    }

    @Nested
    class MultiValueExpressions {

        @Test
        void apply_withArrayWildcard_returnsListOfValues() {
            var evaluator = new JsonPathExpressionEvaluator("{\"values\": [\"a\", \"b\", \"c\"]}");

            var result = evaluator.apply("$.values[*]");

            assertThat(result, is(Optional.of(List.of("a", "b", "c"))));
        }

        @Test
        void apply_withChildWildcard_returnsListOfValues() {
            var evaluator = new JsonPathExpressionEvaluator("{\"values\": [\"1\", \"2\", \"3\"]}");

            var result = evaluator.apply("$.values.*");

            assertThat(result, is(Optional.of(List.of("1", "2", "3"))));
        }

        @Test
        void apply_withEmptyArray_returnsEmpty() {
            var evaluator = new JsonPathExpressionEvaluator("{\"values\": []}");

            var result = evaluator.apply("$.values[*]");

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void apply_withArrayOfNulls_returnsEmpty() {
            var evaluator = new JsonPathExpressionEvaluator("{\"values\": [null, null]}");

            var result = evaluator.apply("$.values[*]");

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void apply_withMixedNullsAndValues_filtersNulls() {
            var evaluator = new JsonPathExpressionEvaluator("{\"values\": [\"a\", null, \"c\"]}");

            var result = evaluator.apply("$.values[*]");

            assertThat(result, is(Optional.of(List.of("a", "c"))));
        }

        @Test
        void apply_withNumericArray_returnsStringRepresentations() {
            var evaluator = new JsonPathExpressionEvaluator("{\"values\": [1, 2, 3]}");

            var result = evaluator.apply("$.values[*]");

            assertThat(result, is(Optional.of(List.of("1", "2", "3"))));
        }
    }

    @Nested
    class NestedStructures {

        @Test
        void apply_withNestedObjectArray_skipsComplexValues() {
            var evaluator = new JsonPathExpressionEvaluator("{\"items\": [{\"name\": \"a\"}, {\"name\": \"b\"}]}");

            var result = evaluator.apply("$.items[*]");

            assertThat(result, is(Optional.empty()));
        }

        @Test
        void apply_withNestedPath_returnsLeafValues() {
            var evaluator = new JsonPathExpressionEvaluator("{\"items\": [{\"name\": \"a\"}, {\"name\": \"b\"}]}");

            var result = evaluator.apply("$.items[*].name");

            assertThat(result, is(Optional.of(List.of("a", "b"))));
        }
    }

    @Nested
    class ReuseAcrossMultipleEvaluations {

        @Test
        void apply_calledMultipleTimes_returnsCorrectResults() {
            var evaluator = new JsonPathExpressionEvaluator("{\"id\": \"x\", \"values\": [\"1\", \"2\"]}");

            assertThat(evaluator.apply("$.id"), is(Optional.of("x")));
            assertThat(evaluator.apply("$.values[*]"), is(Optional.of(List.of("1", "2"))));
            assertThat(evaluator.apply("$.missing"), is(Optional.empty()));
        }
    }
}
