package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
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

        @Test
        void analyze_sliceWithStep_extractsAllThreeParts() {
            var result = JsonPathAnalyzer.analyze("$.items[0:5:2]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(0));
            assertThat(result.slices().get(0).end(), is(5));
            assertThat(result.slices().get(0).step(), is(2));
        }

        @Test
        void analyze_sliceWithStepOnly_extractsStepWithNullBounds() {
            var result = JsonPathAnalyzer.analyze("$.items[::2]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(nullValue()));
            assertThat(result.slices().get(0).end(), is(nullValue()));
            assertThat(result.slices().get(0).step(), is(2));
        }

        @Test
        void analyze_sliceWithStartAndStep_extractsCorrectly() {
            var result = JsonPathAnalyzer.analyze("$.items[1::3]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(1));
            assertThat(result.slices().get(0).end(), is(nullValue()));
            assertThat(result.slices().get(0).step(), is(3));
        }

        @Test
        void analyze_sliceWithoutStep_hasNullStep() {
            var result = JsonPathAnalyzer.analyze("$.items[0:3]");

            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).step(), is(nullValue()));
        }

        @Test
        void analyze_negativeStartSlice_extractsNegativeStart() {
            var result = JsonPathAnalyzer.analyze("$.items[-2:]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(-2));
            assertThat(result.slices().get(0).end(), is(nullValue()));
            assertThat(result.slices().get(0).step(), is(nullValue()));
        }

        @Test
        void analyze_negativeEndSlice_extractsNegativeEnd() {
            var result = JsonPathAnalyzer.analyze("$.items[:-1]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(nullValue()));
            assertThat(result.slices().get(0).end(), is(-1));
            assertThat(result.slices().get(0).step(), is(nullValue()));
        }

        @Test
        void analyze_bothNegativeSlice_extractsBothNegative() {
            var result = JsonPathAnalyzer.analyze("$.items[-3:-1]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.slices(), hasSize(1));
            assertThat(result.slices().get(0).start(), is(-3));
            assertThat(result.slices().get(0).end(), is(-1));
            assertThat(result.slices().get(0).step(), is(nullValue()));
        }
    }

    @Nested
    class Unions {

        @Test
        void analyze_indexUnion_extractsIndices() {
            var result = JsonPathAnalyzer.analyze("$.items[0,2,5]");

            assertThat(result.unions(), hasSize(1));
            assertThat(result.unions().get(0), instanceOf(JsonPathAnalyzer.IndexUnion.class));

            var union = (JsonPathAnalyzer.IndexUnion) result.unions().get(0);
            assertThat(union.indices(), is(List.of(0, 2, 5)));
        }

        @Test
        void analyze_nameUnion_extractsNames() {
            var result = JsonPathAnalyzer.analyze("$.obj['name','age']");

            assertThat(result.unions(), hasSize(1));
            assertThat(result.unions().get(0), instanceOf(JsonPathAnalyzer.NameUnion.class));

            var union = (JsonPathAnalyzer.NameUnion) result.unions().get(0);
            assertThat(union.names(), is(List.of("name", "age")));
        }

        @Test
        void analyze_singleIndex_noUnion() {
            var result = JsonPathAnalyzer.analyze("$.items[0]");

            assertThat(result.unions(), is(empty()));
        }

        @Test
        void analyze_nameUnionWithDoubleQuotes_extractsNames() {
            var result = JsonPathAnalyzer.analyze("$.obj[\"name\",\"age\"]");

            assertThat(result.unions(), hasSize(1));
            assertThat(result.unions().get(0), instanceOf(JsonPathAnalyzer.NameUnion.class));

            var union = (JsonPathAnalyzer.NameUnion) result.unions().get(0);
            assertThat(union.names(), is(List.of("name", "age")));
        }

        @Test
        void analyze_indexUnion_basePath() {
            var result = JsonPathAnalyzer.analyze("$.items[0,2,5]");

            assertThat(result.basePath(), is("$.items[*]"));
        }

        @Test
        void analyze_nameUnion_basePath() {
            var result = JsonPathAnalyzer.analyze("$.obj['name','age']");

            assertThat(result.basePath(), is("$.obj[*]"));
        }

        @Test
        void analyze_wildcardWithNoUnion_returnsEmptyUnionsList() {
            var result = JsonPathAnalyzer.analyze("$.items[*]");

            assertThat(result.unions(), is(empty()));
        }
    }

    @Nested
    class CompoundFilters {

        @Test
        void analyze_andFilter_extractsAndCondition() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.a > 1 && @.b < 2)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.AndFilter.class));

            var andFilter = (JsonPathAnalyzer.AndFilter) result.filters().get(0);
            assertThat(andFilter.left(), instanceOf(JsonPathAnalyzer.GreaterThanNum.class));
            assertThat(andFilter.right(), instanceOf(JsonPathAnalyzer.LessThanNum.class));

            var left = (JsonPathAnalyzer.GreaterThanNum) andFilter.left();
            assertThat(left.fieldJsonPath(), is("$.a"));
            assertThat(left.value(), is(new BigDecimal("1")));

            var right = (JsonPathAnalyzer.LessThanNum) andFilter.right();
            assertThat(right.fieldJsonPath(), is("$.b"));
            assertThat(right.value(), is(new BigDecimal("2")));
        }

        @Test
        void analyze_orFilter_extractsOrCondition() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.a > 1 || @.b < 2)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.OrFilter.class));

            var orFilter = (JsonPathAnalyzer.OrFilter) result.filters().get(0);
            assertThat(orFilter.left(), instanceOf(JsonPathAnalyzer.GreaterThanNum.class));
            assertThat(orFilter.right(), instanceOf(JsonPathAnalyzer.LessThanNum.class));

            var left = (JsonPathAnalyzer.GreaterThanNum) orFilter.left();
            assertThat(left.fieldJsonPath(), is("$.a"));
            assertThat(left.value(), is(new BigDecimal("1")));

            var right = (JsonPathAnalyzer.LessThanNum) orFilter.right();
            assertThat(right.fieldJsonPath(), is("$.b"));
            assertThat(right.value(), is(new BigDecimal("2")));
        }

        @Test
        void analyze_notFilter_extractsNotCondition() {
            var result = JsonPathAnalyzer.analyze("$.items[?(!(@.active == true))]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.NotFilter.class));

            var notFilter = (JsonPathAnalyzer.NotFilter) result.filters().get(0);
            assertThat(notFilter.condition(), instanceOf(JsonPathAnalyzer.EqualBool.class));

            var inner = (JsonPathAnalyzer.EqualBool) notFilter.condition();
            assertThat(inner.fieldJsonPath(), is("$.active"));
            assertThat(inner.value(), is(true));
        }

        @Test
        void analyze_nestedCompound_extractsCorrectTree() {
            // The JSurfer grammar gives && higher precedence than ||, so this expression
            // is parsed as: (@.a > 1 && @.b < 2) || (@.c == 'x')
            var result = JsonPathAnalyzer.analyze("$.items[?(@.a > 1 && @.b < 2 || @.c == 'x')]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.OrFilter.class));

            var orFilter = (JsonPathAnalyzer.OrFilter) result.filters().get(0);
            assertThat(orFilter.left(), instanceOf(JsonPathAnalyzer.AndFilter.class));
            assertThat(orFilter.right(), instanceOf(JsonPathAnalyzer.EqualStr.class));

            var andFilter = (JsonPathAnalyzer.AndFilter) orFilter.left();
            assertThat(andFilter.left(), instanceOf(JsonPathAnalyzer.GreaterThanNum.class));
            assertThat(andFilter.right(), instanceOf(JsonPathAnalyzer.LessThanNum.class));

            var gtFilter = (JsonPathAnalyzer.GreaterThanNum) andFilter.left();
            assertThat(gtFilter.fieldJsonPath(), is("$.a"));
            assertThat(gtFilter.value(), is(new BigDecimal("1")));

            var ltFilter = (JsonPathAnalyzer.LessThanNum) andFilter.right();
            assertThat(ltFilter.fieldJsonPath(), is("$.b"));
            assertThat(ltFilter.value(), is(new BigDecimal("2")));

            var eqFilter = (JsonPathAnalyzer.EqualStr) orFilter.right();
            assertThat(eqFilter.fieldJsonPath(), is("$.c"));
            assertThat(eqFilter.value(), is("x"));
        }
    }

    @Nested
    class FunctionExtensions {

        @Test
        void length_withComparison_parsesLengthCompare() {
            var result = JsonPathAnalyzer.analyze("$.items[?(length(@.tags) > 2)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.LengthCompare.class));

            var filter = (JsonPathAnalyzer.LengthCompare) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.tags"));
            assertThat(filter.op(), is(JsonPathAnalyzer.CompOp.GT));
            assertThat(filter.value(), is(new BigDecimal("2")));
        }

        @Test
        void count_withComparison_parsesLengthCompare() {
            var result = JsonPathAnalyzer.analyze("$.items[?(count(@.tags[*]) == 3)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.LengthCompare.class));

            var filter = (JsonPathAnalyzer.LengthCompare) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.tags"));
            assertThat(filter.op(), is(JsonPathAnalyzer.CompOp.EQ));
            assertThat(filter.value(), is(new BigDecimal("3")));
        }

        @Test
        void match_withPattern_parsesFullMatch() {
            var result = JsonPathAnalyzer.analyze("$.items[?(match(@.name, 'foo.*'))]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.FullMatch.class));

            var filter = (JsonPathAnalyzer.FullMatch) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.name"));
            assertThat(filter.pattern(), is("foo.*"));
        }

        @Test
        void search_withPattern_parsesPartialMatch() {
            var result = JsonPathAnalyzer.analyze("$.items[?(search(@.name, 'bar'))]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.PartialMatch.class));

            var filter = (JsonPathAnalyzer.PartialMatch) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.name"));
            assertThat(filter.pattern(), is("bar"));
        }

        @Test
        void value_rewrittenToBarePath() {
            var result = JsonPathAnalyzer.analyze("$.items[?(value(@.price) > 10)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            // value() is rewritten to bare path, so this becomes a GreaterThanNum
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.GreaterThanNum.class));

            var filter = (JsonPathAnalyzer.GreaterThanNum) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.price"));
            assertThat(filter.value(), is(new BigDecimal("10")));
        }

        @Test
        void length_withUnsupportedOp_parsesCorrectly() {
            var result = JsonPathAnalyzer.analyze("$.items[?(length(@.tags) >= 5)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.LengthCompare.class));

            var filter = (JsonPathAnalyzer.LengthCompare) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.tags"));
            assertThat(filter.op(), is(JsonPathAnalyzer.CompOp.GTE));
            assertThat(filter.value(), is(new BigDecimal("5")));
        }

        @Test
        void function_inCompoundFilter() {
            var result = JsonPathAnalyzer.analyze("$.items[?(length(@.tags) > 0 && match(@.name, 'foo'))]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.AndFilter.class));

            var andFilter = (JsonPathAnalyzer.AndFilter) result.filters().get(0);
            assertThat(andFilter.left(), instanceOf(JsonPathAnalyzer.LengthCompare.class));
            assertThat(andFilter.right(), instanceOf(JsonPathAnalyzer.FullMatch.class));

            var left = (JsonPathAnalyzer.LengthCompare) andFilter.left();
            assertThat(left.fieldJsonPath(), is("$.tags"));
            assertThat(left.op(), is(JsonPathAnalyzer.CompOp.GT));
            assertThat(left.value(), is(new BigDecimal("0")));

            var right = (JsonPathAnalyzer.FullMatch) andFilter.right();
            assertThat(right.fieldJsonPath(), is("$.name"));
            assertThat(right.pattern(), is("foo"));
        }

        @Test
        void match_withDoubleQuotedPattern_parsesFullMatch() {
            var result = JsonPathAnalyzer.analyze("$.items[?(match(@.name, \"test.*\"))]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.FullMatch.class));

            var filter = (JsonPathAnalyzer.FullMatch) result.filters().get(0);
            assertThat(filter.fieldJsonPath(), is("$.name"));
            assertThat(filter.pattern(), is("test.*"));
        }
    }

    @Nested
    class RewrittenOperators {

        @Test
        void analyze_notEqualString_producesNotEqualStr() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.type != 'book')]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.NotFilter.class));

            var notFilter = (JsonPathAnalyzer.NotFilter) result.filters().get(0);
            assertThat(notFilter.condition(), instanceOf(JsonPathAnalyzer.EqualStr.class));

            var inner = (JsonPathAnalyzer.EqualStr) notFilter.condition();
            assertThat(inner.fieldJsonPath(), is("$.type"));
            assertThat(inner.value(), is("book"));
        }

        @Test
        void analyze_notEqualNum_producesNotEqualNum() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.price != 10)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.NotFilter.class));

            var notFilter = (JsonPathAnalyzer.NotFilter) result.filters().get(0);
            assertThat(notFilter.condition(), instanceOf(JsonPathAnalyzer.EqualNum.class));

            var inner = (JsonPathAnalyzer.EqualNum) notFilter.condition();
            assertThat(inner.fieldJsonPath(), is("$.price"));
            assertThat(inner.value(), is(new BigDecimal("10")));
        }

        @Test
        void analyze_notEqualBool_producesNotEqualBool() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.active != true)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.NotFilter.class));

            var notFilter = (JsonPathAnalyzer.NotFilter) result.filters().get(0);
            assertThat(notFilter.condition(), instanceOf(JsonPathAnalyzer.EqualBool.class));

            var inner = (JsonPathAnalyzer.EqualBool) notFilter.condition();
            assertThat(inner.fieldJsonPath(), is("$.active"));
            assertThat(inner.value(), is(true));
        }

        @Test
        void analyze_greaterThanOrEqual_producesNotLessThan() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.price >= 10)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.NotFilter.class));

            var notFilter = (JsonPathAnalyzer.NotFilter) result.filters().get(0);
            assertThat(notFilter.condition(), instanceOf(JsonPathAnalyzer.LessThanNum.class));

            var inner = (JsonPathAnalyzer.LessThanNum) notFilter.condition();
            assertThat(inner.fieldJsonPath(), is("$.price"));
            assertThat(inner.value(), is(new BigDecimal("10")));
        }

        @Test
        void analyze_lessThanOrEqual_producesNotGreaterThan() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.price <= 50)]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.NotFilter.class));

            var notFilter = (JsonPathAnalyzer.NotFilter) result.filters().get(0);
            assertThat(notFilter.condition(), instanceOf(JsonPathAnalyzer.GreaterThanNum.class));

            var inner = (JsonPathAnalyzer.GreaterThanNum) notFilter.condition();
            assertThat(inner.fieldJsonPath(), is("$.price"));
            assertThat(inner.value(), is(new BigDecimal("50")));
        }

        @Test
        void analyze_compoundWithRewrittenOp_parsesCorrectly() {
            var result = JsonPathAnalyzer.analyze("$.items[?(@.price >= 10 && @.type != 'sale')]");

            assertThat(result.basePath(), is("$.items[*]"));
            assertThat(result.filters(), hasSize(1));
            assertThat(result.filters().get(0), instanceOf(JsonPathAnalyzer.AndFilter.class));

            var andFilter = (JsonPathAnalyzer.AndFilter) result.filters().get(0);
            assertThat(andFilter.left(), instanceOf(JsonPathAnalyzer.NotFilter.class));
            assertThat(andFilter.right(), instanceOf(JsonPathAnalyzer.NotFilter.class));

            var leftNot = (JsonPathAnalyzer.NotFilter) andFilter.left();
            assertThat(leftNot.condition(), instanceOf(JsonPathAnalyzer.LessThanNum.class));

            var ltInner = (JsonPathAnalyzer.LessThanNum) leftNot.condition();
            assertThat(ltInner.fieldJsonPath(), is("$.price"));
            assertThat(ltInner.value(), is(new BigDecimal("10")));

            var rightNot = (JsonPathAnalyzer.NotFilter) andFilter.right();
            assertThat(rightNot.condition(), instanceOf(JsonPathAnalyzer.EqualStr.class));

            var eqInner = (JsonPathAnalyzer.EqualStr) rightNot.condition();
            assertThat(eqInner.fieldJsonPath(), is("$.type"));
            assertThat(eqInner.value(), is("sale"));
        }
    }

    @Nested
    class SyntaxValidation {

        @Test
        void analyze_trailingBracket_throwsIllegalArgument() {
            var ex = assertThrows(IllegalArgumentException.class, () -> JsonPathAnalyzer.analyze("$.students[*]]"));
            assertThat(ex.getMessage(), containsString("Invalid JSONPath expression"));
            assertThat(ex.getMessage(), containsString("$.students[*]]"));
        }

        @Test
        void analyze_missingDollarPrefix_throwsIllegalArgument() {
            var ex = assertThrows(IllegalArgumentException.class, () -> JsonPathAnalyzer.analyze("students[*]"));
            assertThat(ex.getMessage(), containsString("Invalid JSONPath expression"));
        }

        @Test
        void analyze_nonsenseInput_throwsIllegalArgument() {
            var ex = assertThrows(
                    IllegalArgumentException.class, () -> JsonPathAnalyzer.analyze("Dhkef;esfkdleshfjdls;fk"));
            assertThat(ex.getMessage(), containsString("Invalid JSONPath expression"));
        }
    }
}
