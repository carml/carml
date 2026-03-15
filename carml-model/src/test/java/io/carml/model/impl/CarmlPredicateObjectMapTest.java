package io.carml.model.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;

import java.util.List;
import org.junit.jupiter.api.Test;

class CarmlPredicateObjectMapTest {

    @Test
    void getReferenceExpressionSet_includesGatheredObjectMapExpressions() {
        // Given
        var gatheredObjectMap = CarmlObjectMap.builder().reference("$.values.*").build();

        var gatherObjectMap =
                CarmlObjectMap.builder().gathers(List.of(gatheredObjectMap)).build();

        var predicateObjectMap = CarmlPredicateObjectMap.builder()
                .predicateMap(
                        CarmlPredicateMap.builder().reference("predicate_ref").build())
                .objectMap(gatherObjectMap)
                .build();

        // When
        var expressionSet = predicateObjectMap.getReferenceExpressionSet();

        // Then
        assertThat(expressionSet, containsInAnyOrder("predicate_ref", "$.values.*"));
    }

    @Test
    void getReferenceExpressionSet_includesDirectAndGatheredExpressions() {
        // Given
        var gatheredObjectMap = CarmlObjectMap.builder().reference("$.items.*").build();

        var objectMapWithGather = CarmlObjectMap.builder()
                .reference("$.direct")
                .gathers(List.of(gatheredObjectMap))
                .build();

        var predicateObjectMap = CarmlPredicateObjectMap.builder()
                .predicateMap(
                        CarmlPredicateMap.builder().reference("predicate_ref").build())
                .objectMap(objectMapWithGather)
                .build();

        // When
        var expressionSet = predicateObjectMap.getReferenceExpressionSet();

        // Then
        assertThat(expressionSet, containsInAnyOrder("predicate_ref", "$.direct", "$.items.*"));
    }

    @Test
    void getReferenceExpressionSet_returnsEmpty_givenNoGathersAndNoExpressions() {
        // Given
        var predicateObjectMap = CarmlPredicateObjectMap.builder().build();

        // When
        var expressionSet = predicateObjectMap.getReferenceExpressionSet();

        // Then
        assertThat(expressionSet, is(empty()));
    }

    @Test
    void getReferenceExpressionSet_handlesEmptyGathersList() {
        // Given
        var objectMapWithEmptyGathers =
                CarmlObjectMap.builder().reference("$.value").build();

        var predicateObjectMap = CarmlPredicateObjectMap.builder()
                .objectMap(objectMapWithEmptyGathers)
                .build();

        // When
        var expressionSet = predicateObjectMap.getReferenceExpressionSet();

        // Then
        assertThat(expressionSet, containsInAnyOrder("$.value"));
    }
}
