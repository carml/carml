package io.carml.jsonpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class JsonPathValidatorTest {

    @Nested
    class ValidateTests {

        @ParameterizedTest
        @NullAndEmptySource
        void validate_withNullOrEmpty_throwsException(String expression) {
            var exception =
                    assertThrows(JsonPathValidationException.class, () -> JsonPathValidator.validate(expression));
            assertThat(exception.getMessage(), is("Invalid JSONPath expression: expression must not be null or empty"));
        }

        @ParameterizedTest
        @ValueSource(strings = {"$.name", "$.food[*]", "$.items[0]", "$['key']", "$.a.b.c", "$..name", "$.*"})
        void validate_withValidDollarPrefixed_doesNotThrow(String expression) {
            assertDoesNotThrow(() -> JsonPathValidator.validate(expression));
        }

        @ParameterizedTest
        @ValueSource(strings = {"name", "Country Code", "my-field", "tags[*]", "@.id"})
        void validate_withValidBareExpression_doesNotThrow(String expression) {
            assertDoesNotThrow(() -> JsonPathValidator.validate(expression));
        }

        @Test
        void validate_withGibberishContainingSemicolons_throwsException() {
            var exception = assertThrows(
                    JsonPathValidationException.class, () -> JsonPathValidator.validate("Dhkef;esfkdleshfjdls;fk"));
            assertThat(exception.getMessage(), startsWith("Invalid JSONPath expression: Dhkef;esfkdleshfjdls;fk"));
        }

        @Test
        void validate_withInvalidDollarPrefixed_throwsException() {
            var exception = assertThrows(JsonPathValidationException.class, () -> JsonPathValidator.validate("$$$$"));
            assertThat(exception.getMessage(), startsWith("Invalid JSONPath expression: $$$$"));
        }

        @Test
        void validate_withInvalidBracketSyntax_throwsException() {
            var exception =
                    assertThrows(JsonPathValidationException.class, () -> JsonPathValidator.validate("$.foo[invalid]"));
            assertThat(exception.getMessage(), startsWith("Invalid JSONPath expression: $.foo[invalid]"));
        }
    }

    @Nested
    class ValidateStrictTests {

        @ParameterizedTest
        @NullAndEmptySource
        void validateStrict_withNullOrEmpty_throwsException(String expression) {
            var exception =
                    assertThrows(JsonPathValidationException.class, () -> JsonPathValidator.validateStrict(expression));
            assertThat(exception.getMessage(), is("Invalid JSONPath expression: expression must not be null or empty"));
        }

        @ParameterizedTest
        @ValueSource(strings = {"$.name", "$.food[*]", "$.items[0]", "$['key']"})
        void validateStrict_withValidDollarPrefixed_doesNotThrow(String expression) {
            assertDoesNotThrow(() -> JsonPathValidator.validateStrict(expression));
        }

        @ParameterizedTest
        @ValueSource(strings = {"name", "Country Code", "my-field"})
        void validateStrict_withValidRelativeKey_doesNotThrow(String expression) {
            assertDoesNotThrow(() -> JsonPathValidator.validateStrict(expression));
        }

        @Test
        void validateStrict_withSemicolons_throwsException() {
            var exception = assertThrows(
                    JsonPathValidationException.class,
                    () -> JsonPathValidator.validateStrict("Dhkef;esfkdleshfjdls;fk"));
            assertThat(
                    exception.getMessage(),
                    is("Invalid JSONPath reference expression 'Dhkef;esfkdleshfjdls;fk': contains invalid characters"));
        }
    }
}
