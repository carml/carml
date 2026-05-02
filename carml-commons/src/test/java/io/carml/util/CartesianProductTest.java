package io.carml.util;

import static io.carml.util.CartesianProduct.cartesianProductStream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CartesianProductTest {

    /**
     * Provides both implementations under test ({@link CartesianProduct#listCartesianProduct} and
     * {@link CartesianProduct#cartesianProductStream}) as adapters to {@code List<List<T>>} so the
     * existing assertions can run against either.
     */
    static Stream<Function<List<List<String>>, List<List<String>>>> implementations() {
        return Stream.of(
                CartesianProduct::listCartesianProduct,
                lists -> cartesianProductStream(lists).toList());
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenEmptyOuterList_whenCartesianProduct_thenReturnSingleEmptyCombination(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of());

        assertThat(result, hasSize(1));
        assertThat(result.get(0), is(empty()));
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenSingleEmptySubList_whenCartesianProduct_thenReturnEmptyResult(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of(List.of()));

        assertThat(result, is(empty()));
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenEmptySubListAmongNonEmpty_whenCartesianProduct_thenReturnEmptyResult(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of(List.of("a"), List.of()));

        assertThat(result, is(empty()));
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenSingleSubList_whenCartesianProduct_thenReturnWrappedElements(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of(List.of("a", "b")));

        assertThat(result, contains(List.of("a"), List.of("b")));
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenTwoSubLists_whenCartesianProduct_thenReturnCorrectOrderedProduct(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of(List.of("a", "b"), List.of("1", "2")));

        assertThat(result, contains(List.of("a", "1"), List.of("a", "2"), List.of("b", "1"), List.of("b", "2")));
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenThreeSubLists_whenCartesianProduct_thenReturnCorrectOrderedProduct(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of(List.of("a", "b"), List.of("1"), List.of("x", "y")));

        assertThat(
                result,
                contains(
                        List.of("a", "1", "x"),
                        List.of("a", "1", "y"),
                        List.of("b", "1", "x"),
                        List.of("b", "1", "y")));
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenDuplicatesInSubList_whenCartesianProduct_thenDuplicatesPreserved(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of(List.of("a", "a")));

        assertThat(result, contains(List.of("a"), List.of("a")));
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenDuplicatesAcrossSubLists_whenCartesianProduct_thenDuplicateCombinationsPreserved(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of(List.of("a", "a"), List.of("x")));

        assertThat(result, contains(List.of("a", "x"), List.of("a", "x")));
    }

    @ParameterizedTest
    @MethodSource("implementations")
    void givenSingleElementSubLists_whenCartesianProduct_thenReturnSingleCombination(
            Function<List<List<String>>, List<List<String>>> impl) {
        var result = impl.apply(List.of(List.of("a"), List.of("1")));

        assertThat(result, contains(List.of("a", "1")));
    }

    @Test
    void cartesianProductStream_isLazy_doesNotMaterializeFullProduct() {
        // Two 10K-element sublists -> 10^8 total tuples; materializing would OOM at typical heap.
        // Limit to 100 and assert wall time is well under what eager construction would take.
        var inputs = List.of(
                IntStream.range(0, 10_000).boxed().toList(),
                IntStream.range(0, 10_000).boxed().toList());

        long start = System.nanoTime();
        long count = cartesianProductStream(inputs).limit(100).count();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;

        assertThat(count, is(100L));
        // Generous bound: materializing 10^8 tuples on any modern hardware would take well over 100 ms,
        // and the laziness contract guarantees we only build 100 tuples here.
        assertThat(elapsedMillis < 1000L, is(true));
    }

    @Test
    void cartesianProductStream_overflowingTotalSize_throwsArithmeticException() {
        // Five 10^5-element sublists -> 10^25 total tuples, which overflows long (max ~9.2 * 10^18).
        var hundredK = IntStream.range(0, 100_000).boxed().toList();
        var inputs = List.of(hundredK, hundredK, hundredK, hundredK, hundredK);

        assertThrows(ArithmeticException.class, () -> cartesianProductStream(inputs));
    }
}
