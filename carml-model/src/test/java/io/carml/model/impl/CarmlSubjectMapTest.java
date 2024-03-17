package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import io.carml.model.impl.template.TemplateParser;
import org.junit.jupiter.api.Test;

class CarmlSubjectMapTest {

    @Test
    void givenSubjectMapWithConstant_whenApplyExpressionAdapter_thenReturnSameSubjectMap() {
        // Given
        var subjectMap = CarmlSubjectMap.builder().constant(literal("foo")).build();

        // When
        var adaptedSubjectMap = subjectMap.applyExpressionAdapter(ref -> "bar");

        // Then
        assertThat(adaptedSubjectMap, is(subjectMap));
    }

    @Test
    void givenSubjectMapWithReference_whenApplyExpressionAdapter_thenReturnAdaptedSubjectMap() {
        // Given
        var subjectMap = CarmlSubjectMap.builder().reference("foo").build();

        // When
        var adaptedSubjectMap = subjectMap.applyExpressionAdapter(ref -> "bar");

        // Then
        var expected = CarmlSubjectMap.builder().reference("bar").build();

        assertThat(adaptedSubjectMap, is(expected));
    }

    @Test
    void givenSubjectMap_whenAdaptTemplate_thenReturnAdaptedSubjectMap() {
        // Given
        var subjectMap = CarmlSubjectMap.builder()
                .template(TemplateParser.getInstance().parse("{foo}-{bar}"))
                .build();

        // When
        var adaptedSubjectMap = subjectMap.applyExpressionAdapter(ref -> String.format("%s-baz", ref));

        // Then
        var expected = CarmlSubjectMap.builder()
                .template(TemplateParser.getInstance().parse("{foo-baz}-{bar-baz}"))
                .build();

        assertThat(adaptedSubjectMap, is(expected));
    }

    @Test
    void givenSubjectMap_whenAdaptFunctionValue_thenReturnAdaptedSubjectMap() {
        // Given
        var subjectMap = CarmlSubjectMap.builder()
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
        var adaptedSubjectMap = subjectMap.applyExpressionAdapter(ref -> String.format("%s-baz", ref));

        // Then
        var expected = CarmlTriplesMap.builder()
                .predicateObjectMap(CarmlPredicateObjectMap.builder()
                        .predicateMap(
                                CarmlPredicateMap.builder().reference("foo-baz").build())
                        .objectMap(CarmlObjectMap.builder().reference("bar-baz").build())
                        .build())
                .build();

        assertThat(adaptedSubjectMap.getFunctionValue(), is(expected));
    }
}
