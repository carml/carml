package io.carml.model.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import io.carml.model.impl.template.TemplateParser;
import org.junit.jupiter.api.Test;

class CarmlExpressionMapTest {

  @Test
  void givenExpressionMap_whenAdaptReference_thenApplyAdaptedReference() {
    // Given
    var expressionMap = CarmlSubjectMap.builder()
        .reference("foo")
        .build();

    var builder = CarmlSubjectMap.builder();

    // When
    expressionMap.adaptReference(ref -> "bar", builder::reference);
    var adaptedExpressionMap = builder.build();

    // Then
    assertThat(adaptedExpressionMap.getReference(), is("bar"));
  }

  @Test
  void givenExpressionMap_whenAdaptTemplate_thenApplyAdaptedTemplate() {
    // Given
    var expressionMap = CarmlSubjectMap.builder()
        .template(TemplateParser.getInstance()
            .parse("{foo}-{bar}"))
        .build();

    var builder = CarmlSubjectMap.builder();

    // When
    expressionMap.adaptTemplate(ref -> String.format("%s-baz", ref), builder::template);
    var adaptedExpressionMap = builder.build();

    // Then
    assertThat(adaptedExpressionMap.getTemplate()
        .toTemplateString(), is("{foo-baz}-{bar-baz}"));
  }

  @Test
  void givenExpressionMap_whenAdaptFunctionValue_thenApplyAdaptedFunctionValue() {
    // Given
    var expressionMap = CarmlSubjectMap.builder()
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

    var builder = CarmlSubjectMap.builder();

    // When
    expressionMap.adaptFunctionValue(ref -> String.format("%s-baz", ref), builder::functionValue);
    var adaptedExpressionMap = builder.build();

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

    assertThat(adaptedExpressionMap.getFunctionValue(), is(expected));
  }
}
