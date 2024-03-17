package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import io.carml.model.impl.template.TemplateParser;
import org.junit.jupiter.api.Test;

class CarmlPredicateMapTest {

    @Test
    void givenPredicateMapWithConstant_whenApplyExpressionAdapter_thenReturnSamePredicateMap() {
        // Given
        var predicateMap = CarmlPredicateMap.builder().constant(literal("foo")).build();

        // When
        var adaptedPredicateMap = predicateMap.applyExpressionAdapter(ref -> "bar");

        // Then
        assertThat(adaptedPredicateMap, is(predicateMap));
    }

    @Test
    void givenPredicateMapWithReference_whenApplyExpressionAdapter_thenReturnAdaptedPredicateMap() {
        // Given
        var predicateMap = CarmlPredicateMap.builder().reference("foo").build();

        // When
        var adaptedPredicateMap = predicateMap.applyExpressionAdapter(ref -> "bar");

        // Then
        var expected = CarmlPredicateMap.builder().reference("bar").build();

        assertThat(adaptedPredicateMap, is(expected));
    }

    @Test
    void givenPredicateMap_whenAdaptTemplate_thenReturnAdaptedPredicateMap() {
        // Given
        var predicateMap = CarmlPredicateMap.builder()
                .template(TemplateParser.getInstance().parse("{foo}-{bar}"))
                .build();

        // When
        var adaptedPredicateMap = predicateMap.applyExpressionAdapter(ref -> String.format("%s-baz", ref));

        // Then
        var expected = CarmlPredicateMap.builder()
                .template(TemplateParser.getInstance().parse("{foo-baz}-{bar-baz}"))
                .build();

        assertThat(adaptedPredicateMap, is(expected));
    }

    @Test
    void givenPredicateMap_whenAdaptFunctionValue_thenReturnAdaptedPredicateMap() {
        // Given
        var predicateMap = CarmlPredicateMap.builder()
                .functionValue(CarmlTriplesMap.builder()
                        .predicateObjectMap(CarmlPredicateObjectMap.builder()
                                .predicateMap(CarmlPredicateMap.builder()
                                        .reference("foo")
                                        .build())
                                .objectMap(CarmlObjectMap.builder()
                                        .reference("bar")
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        var adaptedPredicateMap = predicateMap.applyExpressionAdapter(ref -> String.format("%s-baz", ref));

        // Then
        var expected = CarmlTriplesMap.builder()
                .predicateObjectMap(CarmlPredicateObjectMap.builder()
                        .predicateMap(
                                CarmlPredicateMap.builder().reference("foo-baz").build())
                        .objectMap(CarmlObjectMap.builder().reference("bar-baz").build())
                        .build())
                .build();

        assertThat(adaptedPredicateMap.getFunctionValue(), is(expected));
    }
}
