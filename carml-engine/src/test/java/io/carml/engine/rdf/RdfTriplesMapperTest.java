package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Statements.statement;
import static org.eclipse.rdf4j.model.util.Values.bnode;
import static org.eclipse.rdf4j.model.util.Values.getValueFactory;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.carml.engine.FieldOrigin;
import io.carml.engine.MappedValue;
import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.MappingResult;
import io.carml.engine.ResolvedMapping;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalview.ViewIteration;
import io.carml.model.Field;
import io.carml.model.GraphMap;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf.Rml;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith({MockitoExtension.class})
class RdfTriplesMapperTest {

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private TermGenerator<Resource> subjectGenerator;

    @Mock
    private SubjectMap subjectMap;

    @Mock
    private TriplesMap triplesMap2;

    @Mock
    private TermGenerator<Resource> subjectGenerator2;

    @Mock
    private SubjectMap subjectMap2;

    @Mock
    private TermGenerator<IRI> predicateGenerator1;

    @Mock
    private PredicateMap predicateMap1;

    @Mock
    private TermGenerator<Value> objectGenerator1;

    @Mock
    private ObjectMap objectMap1;

    @Mock
    private PredicateObjectMap pom;

    @Mock
    private TermGenerator<Resource> graphGenerator1;

    @Mock
    private GraphMap graphMap1;

    @Mock
    private TermGenerator<Resource> graphGenerator2;

    @Mock
    private GraphMap graphMap2;

    @Mock
    private TermGenerator<Resource> graphGenerator3;

    @Mock
    private GraphMap graphMap3;

    @Mock
    private LogicalSourceResolver<String> logicalSourceResolver;

    @Mock
    private LogicalSourceResolver.ExpressionEvaluationFactory<String> expressionEvaluationFactory;

    @Mock
    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Mock
    private LogicalSourceRecord<?> logicalSourceRecord;

    @Mock
    private ViewIteration viewIteration;

    @BeforeEach
    void setup() {
        lenient().when(logicalSourceResolver.getExpressionEvaluationFactory()).thenReturn(expressionEvaluationFactory);
        when(triplesMap.getSubjectMaps()).thenReturn(Set.of(subjectMap));
        lenient().when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        lenient().when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of(pom));
    }

    @Test
    void givenTriplesMapWithLogicalTable_whenOfCalled_thenConstructRdfTriplesMapper() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        // When
        RdfTriplesMapper<?> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMappingConfig);

        // Then
        assertThat(rdfTriplesMapper, is(not(nullValue())));
        assertThat(rdfTriplesMapper.getTriplesMap(), is(triplesMap));
    }

    @Test
    void givenAllParams_whenOfCalled_thenConstructRdfTriplesMapper() {
        // Given
        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        // When
        RdfTriplesMapper<?> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // Then
        assertThat(rdfTriplesMapper, is(not(nullValue())));
        assertThat(rdfTriplesMapper.getTriplesMap(), is(triplesMap));
    }

    @Test
    void givenNoSubjectMap_whenOfCalled_thenThrowException() {
        // Given
        when(triplesMap.getSubjectMaps()).thenReturn(Set.of());
        when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
        when(triplesMap.getAsResource()).thenReturn(bnode("triplesMap"));
        RdfMapperConfig rdfMapperConfig = mock(RdfMapperConfig.class);

        // When
        Throwable exception = assertThrows(
                TriplesMapperException.class,
                () -> RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig));

        // Then
        assertThat(
                exception.getMessage(),
                startsWith("Subject map must be specified in triples map blank node resource _:triplesMap in:"));
    }

    @Test
    void givenOnlySubjectMapWithClass_whenMap_thenReturnTypeStatement() {
        // Given
        var subject = iri("http://foo.bar/subject");
        MappedValue<Resource> mappedSubject = RdfMappedValue.of(subject);
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(mappedSubject));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        IRI class1 = iri("http://foo.bar/class1");
        when(subjectMap.getClasses()).thenReturn(Set.of(class1));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // When
        var statements = rdfTriplesMapper.map(logicalSourceRecord);

        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Objects.equals(
                statement(subject, RDF.TYPE, class1, null),
                Mono.from(mappedStatement.getResults()).block());

        // Then
        StepVerifier.create(statements).expectNextMatches(expectedStatement).verifyComplete();
    }

    @Test
    void givenSubjectMapThatReturnsNothing_whenMap_thenReturnEmptyFlux() {
        // Given
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // When
        var statements = rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        StepVerifier.create(statements).expectNextCount(0).verifyComplete();
    }

    @Test
    void givenSubjectMapAndPom_whenMap_thenReturnStatements() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(subject1)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

        when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        when(graphGenerator1.apply(any(), any()))
                .thenReturn(List.of(RdfMappedValue.of(subjectGraph1), RdfMappedValue.of(Rml.defaultGraph)));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = getValueFactory().createLiteral("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap2));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap2)).thenReturn(graphGenerator2);
        IRI graph1 = iri("http://foo.bar/graph1");
        when(graphGenerator2.apply(any(), any()))
                .thenReturn(List.of(RdfMappedValue.of(graph1), RdfMappedValue.of(Rml.defaultGraph)));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // When
        var statements = rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Set.of(
                        statement(subject1, predicate1, object1, subjectGraph1),
                        statement(subject1, predicate1, object1, graph1),
                        statement(subject1, predicate1, object1, null))
                .contains(Mono.from(mappedStatement.getResults()).block());

        StepVerifier.create(statements)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void givenMultipleSubjectMapsAndPom_whenMap_thenReturnStatements() {
        // Given
        when(triplesMap.getSubjectMaps()).thenReturn(Set.of(subjectMap, subjectMap2));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap2)).thenReturn(subjectGenerator2);

        IRI subject1 = iri("http://foo.bar/subject1");
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(subject1)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

        IRI subject2 = iri("http://foo.bar/subject2");
        when(subjectGenerator2.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(subject2)));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap2)).thenReturn(subjectGenerator2);

        when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        when(graphGenerator1.apply(any(), any()))
                .thenReturn(List.of(RdfMappedValue.of(subjectGraph1), RdfMappedValue.of(Rml.defaultGraph)));

        when(subjectMap2.getGraphMaps()).thenReturn(Set.of(graphMap2));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap2)).thenReturn(graphGenerator2);
        IRI subjectGraph2 = iri("http://foo.bar/subjectGraph2");
        when(graphGenerator2.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(subjectGraph2)));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = getValueFactory().createLiteral("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap3));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap3)).thenReturn(graphGenerator3);
        IRI graph1 = iri("http://foo.bar/graph1");
        when(graphGenerator3.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(graph1)));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .allowMultipleSubjectMaps(true)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // When
        var statements = rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Set.of(
                        statement(subject1, predicate1, object1, subjectGraph1),
                        statement(subject1, predicate1, object1, graph1),
                        statement(subject1, predicate1, object1, null),
                        statement(subject2, predicate1, object1, subjectGraph2),
                        statement(subject2, predicate1, object1, graph1))
                .contains(Mono.from(mappedStatement.getResults()).block());

        StepVerifier.create(statements)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void givenOnlySubjectMapWithClass_whenMapViewIteration_thenReturnTypeStatement() {
        // Given
        var subject = iri("http://foo.bar/subject");
        MappedValue<Resource> mappedSubject = RdfMappedValue.of(subject);
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(mappedSubject));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        IRI class1 = iri("http://foo.bar/class1");
        when(subjectMap.getClasses()).thenReturn(Set.of(class1));

        when(viewIteration.getKeys()).thenReturn(Set.of("fieldName"));
        when(viewIteration.getIndex()).thenReturn(0);

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // When
        var statements = rdfTriplesMapper.map(viewIteration);

        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Objects.equals(
                statement(subject, RDF.TYPE, class1, null),
                Mono.from(mappedStatement.getResults()).block());

        // Then
        StepVerifier.create(statements).expectNextMatches(expectedStatement).verifyComplete();
    }

    @Test
    void givenSubjectMapThatReturnsNothing_whenMapViewIteration_thenReturnEmptyFlux() {
        // Given
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());

        when(viewIteration.getKeys()).thenReturn(Set.of("fieldName"));
        when(viewIteration.getIndex()).thenReturn(0);

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // When
        var statements = rdfTriplesMapper.map(viewIteration);

        // Then
        StepVerifier.create(statements).expectNextCount(0).verifyComplete();
    }

    @Test
    void givenStrictModeAndViewIteration_whenCheckStrictModeExpressions_thenCompleteWithoutError() {
        // Given — strict mode is active, so referenceExpressions is populated
        when(triplesMap.getReferenceExpressionSet()).thenReturn(Set.of("name"));
        when(subjectGenerator.apply(any(), any()))
                .thenReturn(List.of(RdfMappedValue.of(iri("http://foo.bar/subject"))));

        when(viewIteration.getKeys()).thenReturn(Set.of("name"));
        when(viewIteration.getIndex()).thenReturn(0);

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .strictMode(true)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // When — map via ViewIteration path, then validate
        StepVerifier.create(rdfTriplesMapper.map(viewIteration)).verifyComplete();

        // Then — validate must not produce a false-positive NonExistentReferenceException
        StepVerifier.create(rdfTriplesMapper.checkStrictModeExpressions()).verifyComplete();
    }

    @Test
    void givenImplicitViewResolvedMapping_whenMapViewIterationThrows_thenErrorEnrichedWithImplicitContext() {
        // Given
        when(viewIteration.getKeys()).thenReturn(Set.of("validKey"));
        when(viewIteration.getIndex()).thenReturn(0);

        var field = mock(Field.class);
        var fieldOrigin = FieldOrigin.of("$.name", triplesMap, field);
        when(triplesMap.getResourceName()).thenReturn(":studentMapping");

        var resolvedMapping = mock(ResolvedMapping.class);
        when(resolvedMapping.isImplicitView()).thenReturn(true);
        when(resolvedMapping.getFieldOrigin("nonExistentKey")).thenReturn(Optional.of(fieldOrigin));

        // The subjectGenerator will call apply("nonExistentKey") on the expression evaluation,
        // which triggers the enriching decorator. The exception propagates synchronously through
        // mapEvaluation's stream collection, so map(ViewIteration) throws directly.
        when(subjectGenerator.apply(any(), any())).thenAnswer(invocation -> {
            ExpressionEvaluation eval = invocation.getArgument(0);
            eval.apply("nonExistentKey");
            return List.of();
        });

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);

        // When
        var exception = assertThrows(TriplesMapperException.class, () -> rdfTriplesMapper.map(viewIteration));

        // Then
        assertThat(exception.getMessage(), is("Error evaluating reference '$.name' in TriplesMap :studentMapping"));
        assertThat(exception.getCause(), is(not(nullValue())));
        assertThat(exception.getCause().getMessage(), containsString("nonExistentKey"));
    }

    @Test
    void givenFieldOriginWithTermMap_whenMapViewIterationThrows_thenErrorIncludesTermMapDescription() {
        // Given
        when(viewIteration.getKeys()).thenReturn(Set.of("validKey"));
        when(viewIteration.getIndex()).thenReturn(0);

        var originatingSubjectMap = mock(SubjectMap.class);
        when(originatingSubjectMap.getAsResource()).thenReturn(iri("http://example.org/subjectMap1"));
        when(triplesMap.asRdf()).thenReturn(new org.eclipse.rdf4j.model.impl.LinkedHashModel());

        var field = mock(Field.class);
        var fieldOrigin = FieldOrigin.of("$.name", originatingSubjectMap, triplesMap, field);

        var resolvedMapping = mock(ResolvedMapping.class);
        when(resolvedMapping.isImplicitView()).thenReturn(true);
        when(resolvedMapping.getFieldOrigin("nonExistentKey")).thenReturn(Optional.of(fieldOrigin));

        when(subjectGenerator.apply(any(), any())).thenAnswer(invocation -> {
            ExpressionEvaluation eval = invocation.getArgument(0);
            eval.apply("nonExistentKey");
            return List.of();
        });

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);

        // When
        var exception = assertThrows(TriplesMapperException.class, () -> rdfTriplesMapper.map(viewIteration));

        // Then — error message includes the bounded TermMap description from LogUtil.exception()
        assertThat(exception.getMessage(), startsWith("Error evaluating reference '$.name' in "));
        assertThat(exception.getMessage(), containsString("http://example.org/subjectMap1"));
        assertThat(exception.getCause(), is(not(nullValue())));
    }

    @Test
    void givenExplicitViewResolvedMapping_whenMapViewIterationThrows_thenErrorEnrichedWithExplicitContext() {
        // Given
        when(viewIteration.getKeys()).thenReturn(Set.of("validKey"));
        when(viewIteration.getIndex()).thenReturn(0);

        var field = mock(Field.class);
        var fieldOrigin = FieldOrigin.of("$.name", triplesMap, field);
        when(triplesMap.getResourceName()).thenReturn(":studentMapping");

        var resolvedMapping = mock(ResolvedMapping.class);
        when(resolvedMapping.isImplicitView()).thenReturn(false);
        when(resolvedMapping.getFieldOrigin("name")).thenReturn(Optional.of(fieldOrigin));

        when(subjectGenerator.apply(any(), any())).thenAnswer(invocation -> {
            ExpressionEvaluation eval = invocation.getArgument(0);
            eval.apply("name");
            return List.of();
        });

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);

        // When
        var exception = assertThrows(TriplesMapperException.class, () -> rdfTriplesMapper.map(viewIteration));

        // Then
        assertThat(
                exception.getMessage(),
                is("Error evaluating field 'name' (reference '$.name') in TriplesMap :studentMapping"));
        assertThat(exception.getCause(), is(not(nullValue())));
        assertThat(exception.getCause().getMessage(), containsString("name"));
    }

    @Test
    void givenNoResolvedMapping_whenMapViewIterationThrows_thenOriginalErrorPropagated() {
        // Given — resolvedMapping is NOT set (null by default)
        when(viewIteration.getKeys()).thenReturn(Set.of("validKey"));
        when(viewIteration.getIndex()).thenReturn(0);

        when(subjectGenerator.apply(any(), any())).thenAnswer(invocation -> {
            ExpressionEvaluation eval = invocation.getArgument(0);
            // Reference a key that does not exist in the view iteration keys,
            // which causes ViewIterationExpressionEvaluation to throw.
            eval.apply("nonExistentKey");
            return List.of();
        });

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        // When & Then — the original error propagates unchanged (not a TriplesMapperException)
        var exception = assertThrows(RuntimeException.class, () -> rdfTriplesMapper.map(viewIteration));
        assertThat(exception, is(not(instanceOf(TriplesMapperException.class))));
        assertThat(exception.getMessage(), containsString("nonExistentKey"));
    }

    @Test
    void givenResolvedMappingWithNoFieldOrigin_whenMapViewIterationThrows_thenOriginalErrorPropagated() {
        // Given — resolvedMapping is set but has no matching FieldOrigin for the expression
        when(viewIteration.getKeys()).thenReturn(Set.of("validKey"));
        when(viewIteration.getIndex()).thenReturn(0);

        var resolvedMapping = mock(ResolvedMapping.class);
        when(resolvedMapping.getFieldOrigin("nonExistentKey")).thenReturn(Optional.empty());

        when(subjectGenerator.apply(any(), any())).thenAnswer(invocation -> {
            ExpressionEvaluation eval = invocation.getArgument(0);
            eval.apply("nonExistentKey");
            return List.of();
        });

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);

        // When & Then — no FieldOrigin match, so original error propagates unchanged
        var exception = assertThrows(RuntimeException.class, () -> rdfTriplesMapper.map(viewIteration));
        assertThat(exception, is(not(instanceOf(TriplesMapperException.class))));
        assertThat(exception.getMessage(), containsString("nonExistentKey"));
    }

    // --- Observer wiring ---

    @Test
    void givenObserverAndResolvedMapping_whenMapLogicalSourceRecord_thenOnMappingStartFired() {
        // Given
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        var observer = mock(MappingExecutionObserver.class);
        var resolvedMapping = mock(ResolvedMapping.class);
        rdfTriplesMapper.setObserver(observer);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);

        // When
        rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        verify(observer).onMappingStart(resolvedMapping);
    }

    @Test
    void givenObserverAndResolvedMapping_whenMapLogicalSourceRecordTwice_thenOnMappingStartFiredOnlyOnce() {
        // Given
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        var observer = mock(MappingExecutionObserver.class);
        var resolvedMapping = mock(ResolvedMapping.class);
        rdfTriplesMapper.setObserver(observer);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);

        // When
        rdfTriplesMapper.map(logicalSourceRecord);
        rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        verify(observer, times(1)).onMappingStart(resolvedMapping);
    }

    @Test
    void givenObserverWithoutResolvedMapping_whenMapLogicalSourceRecord_thenOnMappingStartNotFired() {
        // Given — resolvedMapping is NOT set (null by default)
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        var observer = mock(MappingExecutionObserver.class);
        rdfTriplesMapper.setObserver(observer);
        // resolvedMapping NOT set

        // When
        rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        verify(observer, never()).onMappingStart(any());
    }

    @Test
    void givenObserverAndResolvedMapping_whenMapViewIteration_thenOnMappingStartFired() {
        // Given
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());
        when(viewIteration.getKeys()).thenReturn(Set.of("fieldName"));
        when(viewIteration.getIndex()).thenReturn(0);

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        var observer = mock(MappingExecutionObserver.class);
        var resolvedMapping = mock(ResolvedMapping.class);
        rdfTriplesMapper.setObserver(observer);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);

        // When
        rdfTriplesMapper.map(viewIteration);

        // Then
        verify(observer).onMappingStart(resolvedMapping);
    }

    @Test
    void givenDefaultNoOpObserver_whenMapWithResolvedMapping_thenDoesNotThrow() {
        // Given — observer is NOT set (NoOpObserver by default)
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        var resolvedMapping = mock(ResolvedMapping.class);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);
        // observer NOT set — defaults to NoOpObserver

        // When & Then — should not throw
        var statements = rdfTriplesMapper.map(logicalSourceRecord);
        StepVerifier.create(statements).verifyComplete();
    }

    @Test
    void givenObserverWithoutResolvedMapping_whenMapViewIteration_thenOnMappingStartNotFired() {
        // Given — resolvedMapping is NOT set (null by default)
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());
        when(viewIteration.getKeys()).thenReturn(Set.of("fieldName"));
        when(viewIteration.getIndex()).thenReturn(0);

        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        var observer = mock(MappingExecutionObserver.class);
        rdfTriplesMapper.setObserver(observer);
        // resolvedMapping NOT set

        // When
        rdfTriplesMapper.map(viewIteration);

        // Then
        verify(observer, never()).onMappingStart(any());
    }

    @Test
    void givenObserverAndResolvedMapping_whenFireError_thenOnErrorFired() {
        // Given
        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        var observer = mock(MappingExecutionObserver.class);
        var resolvedMapping = mock(ResolvedMapping.class);
        rdfTriplesMapper.setObserver(observer);
        rdfTriplesMapper.setResolvedMapping(resolvedMapping);

        var cause = new RuntimeException("boom");

        // When — null iteration simulates LogicalSourceRecord path
        rdfTriplesMapper.fireError(null, cause);

        // Then
        verify(observer)
                .onError(
                        eq(resolvedMapping),
                        isNull(),
                        argThat(err -> "boom".equals(err.message())
                                && err.cause().isPresent()
                                && err.cause().get() == cause));
    }

    @Test
    void givenObserverWithoutResolvedMapping_whenFireError_thenOnErrorNotFired() {
        // Given — resolvedMapping NOT set
        var rdfMapperConfig = RdfMapperConfig.builder()
                .rdfTermGeneratorConfig(mock(RdfTermGeneratorConfig.class))
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper =
                RdfTriplesMapper.of(triplesMap, logicalSourceResolver, rdfMapperConfig);

        var observer = mock(MappingExecutionObserver.class);
        rdfTriplesMapper.setObserver(observer);
        // resolvedMapping NOT set

        // When
        rdfTriplesMapper.fireError(null, new RuntimeException("boom"));

        // Then
        verify(observer, never()).onError(any(), any(), any());
    }
}
