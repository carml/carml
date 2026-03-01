package io.carml.model.impl;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import io.carml.model.TermType;
import io.carml.model.impl.template.TemplateParser;
import org.junit.jupiter.api.Test;

class CarmlObjectMapTest {

    @Test
    void givenObjectMapWithConstant_whenApplyExpressionAdapter_thenReturnSameObjectMap() {
        // Given
        var objectMap = CarmlObjectMap.builder().constant(literal("foo")).build();

        // When
        var adaptedObjectMap = objectMap.applyExpressionAdapter(ref -> "bar");

        // Then
        assertThat(adaptedObjectMap, is(objectMap));
    }

    @Test
    void givenObjectMapWithReference_whenApplyExpressionAdapter_thenReturnAdaptedObjectMap() {
        // Given
        var objectMap = CarmlObjectMap.builder().reference("foo").build();

        // When
        var adaptedObjectMap = objectMap.applyExpressionAdapter(ref -> "bar");

        // Then
        var expected = CarmlObjectMap.builder().reference("bar").build();

        assertThat(adaptedObjectMap, is(expected));
    }

    @Test
    void givenObjectMap_whenAdaptTemplate_thenReturnAdaptedObjectMap() {
        // Given
        var objectMap = CarmlObjectMap.builder()
                .template(TemplateParser.getInstance().parse("{foo}-{bar}"))
                .build();

        // When
        var adaptedObjectMap = objectMap.applyExpressionAdapter(ref -> String.format("%s-baz", ref));

        // Then
        var expected = CarmlObjectMap.builder()
                .template(TemplateParser.getInstance().parse("{foo-baz}-{bar-baz}"))
                .build();

        assertThat(adaptedObjectMap, is(expected));
    }

    @Test
    void getTermType_returnsLiteral_givenFunctionExecution() {
        var objectMap = CarmlObjectMap.builder()
                .functionExecution(CarmlFunctionExecution.builder().build())
                .build();

        assertThat(objectMap.getTermType(), is(TermType.LITERAL));
    }

    @Test
    void getTermType_returnsIri_givenFunctionValue() {
        var objectMap = CarmlObjectMap.builder()
                .functionValue(CarmlTriplesMap.builder().build())
                .build();

        assertThat(objectMap.getTermType(), is(TermType.IRI));
    }

    @Test
    void getTermType_returnsIri_givenTemplate() {
        var objectMap = CarmlObjectMap.builder()
                .template(TemplateParser.getInstance().parse("http://example.org/{foo}"))
                .build();

        assertThat(objectMap.getTermType(), is(TermType.IRI));
    }

    @Test
    void getTermType_returnsLiteral_givenReference() {
        var objectMap = CarmlObjectMap.builder().reference("foo").build();

        assertThat(objectMap.getTermType(), is(TermType.LITERAL));
    }

    @Test
    void getTermType_returnsExplicitTermType_givenFunctionExecution() {
        var objectMap = CarmlObjectMap.builder()
                .functionExecution(CarmlFunctionExecution.builder().build())
                .termType(TermType.IRI)
                .build();

        assertThat(objectMap.getTermType(), is(TermType.IRI));
    }

    @Test
    void getTermType_returnsLiteral_givenLanguageMap() {
        var objectMap = CarmlObjectMap.builder()
                .constant(iri("http://example.org/foo"))
                .languageMap(CarmlLanguageMap.builder().constant(literal("en")).build())
                .build();

        assertThat(objectMap.getTermType(), is(TermType.LITERAL));
    }

    @Test
    void givenObjectMap_whenAdaptFunctionValue_thenReturnAdaptedObjectMap() {
        // Given
        var objectMap = CarmlObjectMap.builder()
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
        var adaptedObjectMap = objectMap.applyExpressionAdapter(ref -> String.format("%s-baz", ref));

        // Then
        var expected = CarmlTriplesMap.builder()
                .predicateObjectMap(CarmlPredicateObjectMap.builder()
                        .predicateMap(
                                CarmlPredicateMap.builder().reference("foo-baz").build())
                        .objectMap(CarmlObjectMap.builder().reference("bar-baz").build())
                        .build())
                .build();

        assertThat(adaptedObjectMap.getFunctionValue(), is(expected));
    }
}
