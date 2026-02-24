package io.carml.util;

import static io.carml.util.CartesianProduct.listCartesianProduct;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.junit.jupiter.api.Test;

class CartesianProductTest {

    @Test
    void givenEmptyOuterList_whenCartesianProduct_thenReturnSingleEmptyCombination() {
        var result = listCartesianProduct(List.<List<String>>of());

        assertThat(result, hasSize(1));
        assertThat(result.get(0), is(empty()));
    }

    @Test
    void givenSingleEmptySubList_whenCartesianProduct_thenReturnEmptyResult() {
        var result = listCartesianProduct(List.of(List.<String>of()));

        assertThat(result, is(empty()));
    }

    @Test
    void givenEmptySubListAmongNonEmpty_whenCartesianProduct_thenReturnEmptyResult() {
        var result = listCartesianProduct(List.of(List.of("a"), List.of()));

        assertThat(result, is(empty()));
    }

    @Test
    void givenSingleSubList_whenCartesianProduct_thenReturnWrappedElements() {
        var result = listCartesianProduct(List.of(List.of("a", "b")));

        assertThat(result, contains(List.of("a"), List.of("b")));
    }

    @Test
    void givenTwoSubLists_whenCartesianProduct_thenReturnCorrectOrderedProduct() {
        var result = listCartesianProduct(List.of(List.of("a", "b"), List.of("1", "2")));

        assertThat(result, contains(List.of("a", "1"), List.of("a", "2"), List.of("b", "1"), List.of("b", "2")));
    }

    @Test
    void givenThreeSubLists_whenCartesianProduct_thenReturnCorrectOrderedProduct() {
        var result = listCartesianProduct(List.of(List.of("a", "b"), List.of("1"), List.of("x", "y")));

        assertThat(
                result,
                contains(
                        List.of("a", "1", "x"),
                        List.of("a", "1", "y"),
                        List.of("b", "1", "x"),
                        List.of("b", "1", "y")));
    }

    @Test
    void givenDuplicatesInSubList_whenCartesianProduct_thenDuplicatesPreserved() {
        var result = listCartesianProduct(List.of(List.of("a", "a")));

        assertThat(result, contains(List.of("a"), List.of("a")));
    }

    @Test
    void givenDuplicatesAcrossSubLists_whenCartesianProduct_thenDuplicateCombinationsPreserved() {
        var result = listCartesianProduct(List.of(List.of("a", "a"), List.of("x")));

        assertThat(result, contains(List.of("a", "x"), List.of("a", "x")));
    }

    @Test
    void givenSingleElementSubLists_whenCartesianProduct_thenReturnSingleCombination() {
        var result = listCartesianProduct(List.of(List.of("a"), List.of("1")));

        assertThat(result, contains(List.of("a", "1")));
    }
}
