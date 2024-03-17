package io.carml.engine.rdf;

import static io.carml.model.TermType.BLANK_NODE;
import static org.eclipse.rdf4j.model.util.Values.bnode;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.carml.engine.TermGenerator;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.GraphMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.SubjectMap;
import io.carml.model.TermMap;
import io.carml.model.impl.CarmlDatatypeMap;
import io.carml.model.impl.CarmlGraphMap;
import io.carml.model.impl.CarmlLanguageMap;
import io.carml.model.impl.CarmlObjectMap;
import io.carml.model.impl.CarmlPredicateMap;
import io.carml.model.impl.CarmlSubjectMap;
import io.carml.model.impl.template.TemplateParser;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RdfTermGeneratorFactoryTest {

    @Mock
    ExpressionEvaluation expressionEvaluation;

    @Mock
    DatatypeMapper datatypeMapper;

    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @BeforeEach
    void beforeEach() {
        var rdfTermGeneratorConfig = RdfTermGeneratorConfig.builder()
                .baseIri(iri("http://example.com/base/"))
                .valueFactory(SimpleValueFactory.getInstance())
                .normalizationForm(Normalizer.Form.NFC)
                .build();
        rdfTermGeneratorFactory = RdfTermGeneratorFactory.of(rdfTermGeneratorConfig);
    }

    @Test
    void givenRdfTermGenerator_whenExpressionEvaluationReturnsEmpty_thenResultEmpty() {
        // Given
        var objectMap = CarmlObjectMap.builder()
                .id("obj-map-1")
                .template(TemplateParser.getInstance().parse("http://{foo}"))
                .build();

        var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);
        when(expressionEvaluation.apply(any())).thenReturn(Optional.empty());
        when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

        // When
        var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

        // Then
        assertThat(objects, is(empty()));
    }

    @Test
    void givenRdfTermGenerator_whenExpressionEvaluationReturnsListWithOnlyNull_thenResultEmpty() {
        // Given
        var objectMap =
                CarmlObjectMap.builder().id("obj-map-1").reference("foo").build();

        var nullList = new ArrayList<>();
        nullList.add(null);
        var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

        when(expressionEvaluation.apply(any())).thenReturn(Optional.of(nullList));
        when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

        // When
        var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

        // Then
        assertThat(objects, is(empty()));
    }

    @Test
    void givenRdfTermGenerator_whenExpressionEvaluationReturnsListWithNullAndNonNull_thenResultWithoutNulls() {
        // Given
        var nullList = new ArrayList<>();
        nullList.add(null);
        nullList.add("bar");

        var objectMap =
                CarmlObjectMap.builder().id("obj-map-1").reference("foo").build();
        var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

        when(expressionEvaluation.apply(any())).thenReturn(Optional.of(nullList));
        when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

        // When
        var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

        // Then
        assertThat(objects, hasItems(literal("bar")));
    }

    @Test
    void givenObjectMapWithLanguageMap_whenObjectGeneratorApplied_thenReturnLiteralWithLang() {
        // Given
        var objectMap = CarmlObjectMap.builder()
                .id("obj-map-1")
                .reference("foo")
                .languageMap(CarmlLanguageMap.builder().reference("foo").build())
                .build();

        when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("bar")));
        when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

        var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

        // When
        var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

        // Then
        assertThat(objects, hasItems(literal("bar", "bar")));
    }

    @Test
    void givenObjectMapWithDatatypeMap_whenObjectGeneratorApplied_thenReturnLiteralWithDatatype() {
        // Given
        var objectMap = CarmlObjectMap.builder()
                .id("obj-map-1")
                .reference("foo")
                .datatypeMap(CarmlDatatypeMap.builder()
                        .template(TemplateParser.getInstance().parse("https://{foo}.com"))
                        .build())
                .build();

        when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("bar")));
        when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

        var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

        // When
        var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

        // Then
        assertThat(objects, hasItems(literal("bar", iri("https://bar.com"))));
    }

    static Stream<Arguments> termGeneratorTestInput() {
        return Stream.of(
                Arguments.of(
                        CarmlSubjectMap.builder().reference("foo").build(),
                        "foo",
                        List.of("bar"),
                        null,
                        List.of(iri("http://example.com/base/bar"))),
                Arguments.of(
                        CarmlSubjectMap.builder()
                                .reference("foo")
                                .termType(BLANK_NODE)
                                .build(),
                        "foo",
                        List.of("bar"),
                        null,
                        List.of(bnode("bar"))),
                Arguments.of(
                        CarmlPredicateMap.builder()
                                .template(TemplateParser.getInstance().parse("http://example.org/{foo}"))
                                .build(),
                        "foo",
                        List.of(0),
                        XSD.BOOLEAN,
                        List.of(iri("http://example.org/false"))),
                Arguments.of(
                        CarmlPredicateMap.builder()
                                .constant(iri("http://example.org/constant"))
                                .build(),
                        "foo",
                        List.of(),
                        null,
                        List.of(iri("http://example.org/constant"))),
                Arguments.of(
                        CarmlObjectMap.builder()
                                .reference("foo")
                                .languageMap(CarmlLanguageMap.builder()
                                        .reference("foo")
                                        .build())
                                .build(),
                        "foo",
                        List.of("bar", "baz"),
                        XSD.STRING,
                        List.of(
                                literal("bar", "bar"),
                                literal("baz", "baz"),
                                literal("bar", "baz"),
                                literal("baz", "bar"))),
                Arguments.of(
                        CarmlObjectMap.builder()
                                .reference("foo")
                                .datatypeMap(CarmlDatatypeMap.builder()
                                        .template(TemplateParser.getInstance().parse("http://foo.org/{foo}-{foo}"))
                                        .build())
                                .build(),
                        "foo",
                        List.of("bar", "baz"),
                        XSD.STRING,
                        List.of(
                                literal("bar", iri("http://foo.org/bar-bar")),
                                literal("bar", iri("http://foo.org/baz-bar")),
                                literal("bar", iri("http://foo.org/bar-baz")),
                                literal("bar", iri("http://foo.org/baz-baz")),
                                literal("baz", iri("http://foo.org/bar-bar")),
                                literal("baz", iri("http://foo.org/baz-bar")),
                                literal("baz", iri("http://foo.org/bar-baz")),
                                literal("baz", iri("http://foo.org/baz-baz")))),
                Arguments.of(
                        CarmlObjectMap.builder().reference("foo").build(),
                        "foo",
                        List.of("2011-08-23T22:17:00.000+00:00"),
                        XSD.DATETIME,
                        List.of(literal("2011-08-23T22:17:00Z", XSD.DATETIME))),
                Arguments.of(
                        CarmlGraphMap.builder().reference("foo").build(),
                        "foo",
                        List.of("bar"),
                        null,
                        List.of(iri("http://example.com/base/bar"))));
    }

    @ParameterizedTest
    @MethodSource("termGeneratorTestInput")
    void givenTermGenerator_whenApply_thenReturnExpectedValues(
            TermMap termMap,
            String expressionIn,
            List<Object> expressionOut,
            IRI datatypeOut,
            List<Value> expectedTerms) {
        // Given
        TermGenerator<? extends Value> termGenerator;
        if (termMap instanceof SubjectMap subjectMap) {
            termGenerator = rdfTermGeneratorFactory.getSubjectGenerator(subjectMap);
        } else if (termMap instanceof PredicateMap predicateMap) {
            termGenerator = rdfTermGeneratorFactory.getPredicateGenerator(predicateMap);
        } else if (termMap instanceof ObjectMap objectMap) {
            termGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);
        } else if (termMap instanceof GraphMap graphMap) {
            termGenerator = rdfTermGeneratorFactory.getGraphGenerator(graphMap);
        } else {
            throw new IllegalStateException();
        }

        lenient().when(expressionEvaluation.apply(expressionIn)).thenReturn(Optional.of(expressionOut));
        lenient().when(datatypeMapper.apply(any())).thenReturn(Optional.ofNullable(datatypeOut));

        // When
        var terms = termGenerator.apply(expressionEvaluation, datatypeMapper);

        // Then
        assertThat(terms, containsInAnyOrder(expectedTerms.toArray()));
    }
}
