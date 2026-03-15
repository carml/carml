package io.carml.jsonpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class JsonPathSelectorsTest {

    @Nested
    class IsMultiValuedSelectorTests {

        @ParameterizedTest
        @ValueSource(strings = {"$.items[*]", "$.items[*].name", "$.departments[*].employees[*].name"})
        void isMultiValuedSelector_withArrayWildcard_returnsTrue(String expression) {
            assertThat(JsonPathSelectors.isMultiValuedSelector(expression), is(true));
        }

        @ParameterizedTest
        @ValueSource(strings = {"$.*", "$.obj.*", "$.obj.*.label"})
        void isMultiValuedSelector_withChildWildcard_returnsTrue(String expression) {
            assertThat(JsonPathSelectors.isMultiValuedSelector(expression), is(true));
        }

        @ParameterizedTest
        @ValueSource(strings = {"$..name", "$.items..value"})
        void isMultiValuedSelector_withDeepScan_returnsTrue(String expression) {
            assertThat(JsonPathSelectors.isMultiValuedSelector(expression), is(true));
        }

        @ParameterizedTest
        @ValueSource(strings = {"$.name", "name", "$.items[0]", "$.a.b.c"})
        void isMultiValuedSelector_withDefinitePath_returnsFalse(String expression) {
            assertThat(JsonPathSelectors.isMultiValuedSelector(expression), is(false));
        }

        @ParameterizedTest
        @NullAndEmptySource
        void isMultiValuedSelector_withNullOrEmpty_returnsFalse(String expression) {
            assertThat(JsonPathSelectors.isMultiValuedSelector(expression), is(false));
        }
    }

    @Nested
    class IsDefinitePathTests {

        @Test
        void isDefinitePath_withDefinitePath_returnsTrue() {
            assertThat(JsonPathSelectors.isDefinitePath("$.name"), is(true));
        }

        @Test
        void isDefinitePath_withArrayIndex_returnsTrue() {
            assertThat(JsonPathSelectors.isDefinitePath("$.items[0]"), is(true));
        }

        @Test
        void isDefinitePath_withWildcard_returnsFalse() {
            assertThat(JsonPathSelectors.isDefinitePath("$.items[*]"), is(false));
        }

        @Test
        void isDefinitePath_withDeepScan_returnsFalse() {
            assertThat(JsonPathSelectors.isDefinitePath("$..name"), is(false));
        }

        @Test
        void isDefinitePath_withFilter_returnsFalse() {
            assertThat(JsonPathSelectors.isDefinitePath("$.items[?(@.active==true)]"), is(false));
        }
    }
}
