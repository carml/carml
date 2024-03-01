package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import io.carml.model.impl.template.TemplateParser;
import org.junit.jupiter.api.Test;

class CarmlGraphMapTest {

  @Test
  void givenGraphMapWithConstant_whenApplyExpressionAdapter_thenReturnSameGraphMap() {
    // Given
    var graphMap = CarmlGraphMap.builder()
        .constant(literal("foo"))
        .build();

    // When
    var adaptedGraphMap = graphMap.applyExpressionAdapter(ref -> "bar");

    // Then
    assertThat(adaptedGraphMap, is(graphMap));
  }

  @Test
  void givenGraphMapWithReference_whenApplyExpressionAdapter_thenReturnAdaptedGraphMap() {
    // Given
    var graphMap = CarmlGraphMap.builder()
        .reference("foo")
        .build();

    // When
    var adaptedGraphMap = graphMap.applyExpressionAdapter(ref -> "bar");

    // Then
    var expected = CarmlGraphMap.builder()
        .reference("bar")
        .build();

    assertThat(adaptedGraphMap, is(expected));
  }

  @Test
  void givenGraphMap_whenAdaptTemplate_thenReturnAdaptedGraphMap() {
    // Given
    var graphMap = CarmlGraphMap.builder()
        .template(TemplateParser.getInstance()
            .parse("{foo}-{bar}"))
        .build();

    // When
    var adaptedGraphMap = graphMap.applyExpressionAdapter(ref -> String.format("%s-baz", ref));

    // Then
    var expected = CarmlGraphMap.builder()
        .template(TemplateParser.getInstance()
            .parse("{foo-baz}-{bar-baz}"))
        .build();

    assertThat(adaptedGraphMap, is(expected));
  }

  @Test
  void givenGraphMap_whenAdaptFunctionValue_thenReturnAdaptedGraphMap() {
    // Given
    var graphMap = CarmlGraphMap.builder()
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
    var adaptedGraphMap = graphMap.applyExpressionAdapter(ref -> String.format("%s-baz", ref));

    // Then
    var expected = CarmlTriplesMap.builder()
        .predicateObjectMap(CarmlPredicateObjectMap.builder()
            .predicateMap(CarmlPredicateMap.builder()
                .reference("foo-baz")
                .build())
            .objectMap(CarmlObjectMap.builder()
                .reference("bar-baz")
                .build())
            .build())
        .build();

    assertThat(adaptedGraphMap.getFunctionValue(), is(expected));
  }
}
