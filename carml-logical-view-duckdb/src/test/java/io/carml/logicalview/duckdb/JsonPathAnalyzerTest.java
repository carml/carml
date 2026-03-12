package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JsonPathAnalyzerTest {

    @Nested
    class BasePath {

        @Test
        void analyze_simpleWildcard_returnsPathWithNoFilters() {
            var result = JsonPathAnalyzer.analyze("$.people[*]");

            assertThat(result.basePath(), is("$.people[*]"));
            assertThat(result.filters(), is(empty()));
            assertThat(result.hasDeepScan(), is(false));
        }

        @Test
        void analyze_slicing_normalizesToWildcard() {
            var result = JsonPathAnalyzer.analyze("$.items[0:3]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), is(empty()));
            assertThat(result.hasDeepScan(), is(false));
            assertThat(result.slices(), hasSize(1));
        }

        static Stream<Arguments> basePathCases() {
            return Stream.of(
                    Arguments.of("$.store.books[*]", "$.store.books[*]"),
                    Arguments.of("$", "$"),
                    Arguments.of("$.items[0]", "$.items[0]"),
                    Arguments.of("$.store.*", "$.store.*"),
                    Arguments.of("$.items[0,2]", "$.items[*]"),
                    Arguments.of("$['name','age']", "$[*]"),
                    Arguments.of("$*", "$[*]"));
        }

        @ParameterizedTest
        @MethodSource("basePathCases")
        void analyze_returnsExpectedBasePathWithNoFilters(String input, String expectedBasePath) {
            var result = JsonPathAnalyzer.analyze(input);

            assertThat(result.basePath(), is(expectedBasePath));
            assertThat(result.filters(), is(empty()));
        }
    }

    @Nested
    class DeepScan {

        @Test
        void analyze_deepScan_setsHasDeepScanFlag() {
            var result = JsonPathAnalyzer.analyze("$..name");

            assertThat(result.hasDeepScan(), is(true));
            assertThat(result.basePath(), is("$..name"));
        }

        @Test
        void analyze_noDeepScan_doesNotSetFlag() {
            var result = JsonPathAnalyzer.analyze("$.people[*]");

            assertThat(result.hasDeepScan(), is(false));
        }
    }

    @Nested
    class Filters {

        @Test
        void analyze_equalStringFilter_extractsCondition() {
            var result = JsonPathAnalyzer.analyze("$.people[?(@.name == 'alice')]");

            assertThat(result.basePath(), is("$.people[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.EqualStr.class));

            var filter = (JsonPathAnalyzer.EqualStr) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.name"));
            assertThat(filter.value(), is("alice"));
        }

        @Test
        void analyze_equalNumFilter_extractsCondition() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.price == 10)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.EqualNum.class));

            var filter = (JsonPathAnalyzer.EqualNum) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.price"));
            assertThat(filter.value(), is(new BigDecimal("10")));
        }

        @Test
        void analyze_equalBoolFilter_extractsCondition() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.active == true)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.EqualBool.class));

            var filter = (JsonPathAnalyzer.EqualBool) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.active"));
            assertThat(filter.value(), is(true));
        }

        @Test
        void analyze_greaterThanFilter_extractsCondition() {
            var result = JsonPathAnalyzer.analyze("$.data[?(@.value > 25)]");

            assertThat(result.basePath(), is("$.data[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.GreaterThanNum.class));

            var filter = (JsonPathAnalyzer.GreaterThanNum) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.value"));
            assertThat(filter.value(), is(new BigDecimal("25")));
        }

        @Test
        void analyze_lessThanFilter_extractsCondition() {
            var result = JsonPathAnalyzer.analyze("$.data[?(@.value < 50)]");

            assertThat(result.basePath(), is("$.data[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.LessThanNum.class));

            var filter = (JsonPathAnalyzer.LessThanNum) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.value"));
            assertThat(filter.value(), is(new BigDecimal("50")));
        }

        @Test
        void analyze_existsFilter_extractsCondition() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.type)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.Exists.class));

            var filter = (JsonPathAnalyzer.Exists) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.type"));
        }

        @Test
        void analyze_regexFilter_extractsCondition() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.name =~ /^test/)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.MatchRegex.class));

            var filter = (JsonPathAnalyzer.MatchRegex) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.name"));
            assertThat(filter.pattern(), is("^test"));
        }

        @Test
        void analyze_filterReplacedByWildcard_inBasePath() {
            var result = JsonPathAnalyzer.analyze("$.people[?(@.age > 18)].name");

            assertThat(result.basePath(), is("$.people[*].name"));
            assertThat(result.filters(), hasSize(1));
        }

        @Test
        void analyze_nestedFilterField_buildsCorrectPath() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.meta.type == 'book')]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));

            var filter = (JsonPathAnalyzer.EqualStr) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.meta.type"));
        }
    }

    @Nested
    class Slices {

        @Test
        void analyze_sliceWithStartAndEnd_extractsBothBounds() {
            var result = JsonPathAnalyzer.analyze("$.items[0:3]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(0));
            assertThat(result.slices().get(0).end(), is(3));
        }

        @Test
        void analyze_sliceWithStartOnly_extractsStartWithNullEnd() {
            var result = JsonPathAnalyzer.analyze("$.items[1:]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(1));
            assertThat(result.slices().get(0).end(), is(nullValue()));
        }

        @Test
        void analyze_sliceWithEndOnly_extractsEndWithNullStart() {
            var result = JsonPathAnalyzer.analyze("$.items[:2]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(nullValue()));
            assertThat(result.slices().get(0).end(), is(2));
        }

        @Test
        void analyze_sliceWithNoBounds_extractsBothNull() {
            var result = JsonPathAnalyzer.analyze("$.items[:]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(nullValue()));
            assertThat(result.slices().get(0).end(), is(nullValue()));
        }

        @Test
        void analyze_wildcardWithNoSlice_returnsEmptySlicesList() {
            var result = JsonPathAnalyzer.analyze("$.items[*]");

            assertThat(result.slices(), is(empty()));
        }

        @Test
        void analyze_nonZeroStartSlice_extractsCorrectStart() {
            var result = JsonPathAnalyzer.analyze("$.items[2:5]");

            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(2));
            assertThat(result.slices().get(0).end(), is(5));
        }
    }

    @Nested
    class CompoundFilters {

        @Test
        void analyze_andFilter_throwsUnsupportedOperationException() {
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> JsonPathAnalyzer.analyze("$.items[?(@.a > 1 && @.b < 2)]"));
        }

        @Test
        void analyze_orFilter_throwsUnsupportedOperationException() {
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> JsonPathAnalyzer.analyze("$.items[?(@.a > 1 || @.b < 2)]"));
        }
    }
}
