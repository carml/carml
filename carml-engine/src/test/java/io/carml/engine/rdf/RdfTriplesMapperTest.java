package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.engine.join.ParentSideJoinConditionStoreProvider;
import io.carml.engine.join.ParentSideJoinKey;
import io.carml.engine.join.impl.CarmlParentSideJoinConditionStoreProvider;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.model.GraphMap;
import io.carml.model.Join;
import io.carml.model.LogicalTable;
import io.carml.model.ObjectMap;
import io.carml.model.ParentMap;
import io.carml.model.PredicateMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith({MockitoExtension.class})
class RdfTriplesMapperTest {

    private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private TermGenerator<Resource> subjectGenerator;

    @Mock
    private SubjectMap subjectMap;

    @Mock
    private LogicalTable logicalTable;

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

    private Set<RdfRefObjectMapper> refObjectMappers;

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
    private RefObjectMap refObjectMap1;

    @Mock
    private RefObjectMap refObjectMap2;

    @Mock
    private RdfRefObjectMapper rdfRefObjectMapper1;

    @Mock
    private RdfRefObjectMapper rdfRefObjectMapper2;

    private Set<RdfRefObjectMapper> incomingRefObjectMappers;

    @Mock
    private LogicalSourceResolver<String> logicalSourceResolver;

    @Mock
    private LogicalSourceResolver.ExpressionEvaluationFactory<String> expressionEvaluationFactory;

    @Mock
    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Mock
    private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider;

    @Mock
    private ExpressionEvaluation expressionEvaluation;

    @Mock
    private Join join1;

    @Mock
    private ParentMap parentMap1;

    @Mock
    private Join join2;

    @Mock
    private ParentMap parentMap2;

    @Mock
    private Join join3;

    @Mock
    private ParentMap parentMap3;

    @Mock
    private LogicalSourceRecord<?> logicalSourceRecord;

    private ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider;

    @BeforeEach
    void setup() {
        refObjectMappers = new HashSet<>();
        incomingRefObjectMappers = new HashSet<>();
        lenient().when(logicalSourceResolver.getExpressionEvaluationFactory()).thenReturn(expressionEvaluationFactory);
        when(triplesMap.getSubjectMaps()).thenReturn(Set.of(subjectMap));
        lenient().when(triplesMap.getId()).thenReturn("triples-map-1");
        lenient().when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        lenient().when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of(pom));
        parentSideJoinConditionStoreProvider = CarmlParentSideJoinConditionStoreProvider.of();
    }

    @Test
    void givenTriplesMapWithLogicalTable_whenOfCalled_thenConstructRdfTriplesMapper() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> VALUE_FACTORY)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        // When
        RdfTriplesMapper<?> rdfTriplesMapper = RdfTriplesMapper.of(
                triplesMap, refObjectMappers, incomingRefObjectMappers, logicalSourceResolver, rdfMappingConfig);

        // Then
        assertThat(rdfTriplesMapper, is(not(nullValue())));
        assertThat(rdfTriplesMapper.getRefObjectMappers(), is(empty()));
        assertThat(rdfTriplesMapper.getConnectedRefObjectMappers(), is(empty()));
        assertThat(rdfTriplesMapper.getTriplesMap(), is(triplesMap));
    }

    @Test
    void givenAllParams_whenOfCalled_thenConstructRdfTriplesMapper() {
        // Given
        when(triplesMap.getLogicalTable()).thenReturn(logicalTable);
        when(rdfRefObjectMapper1.getTriplesMap()).thenReturn(triplesMap2);
        when(triplesMap2.getLogicalTable()).thenReturn(logicalTable);

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> VALUE_FACTORY)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        incomingRefObjectMappers.add(rdfRefObjectMapper1);

        // When
        RdfTriplesMapper<?> rdfTriplesMapper = RdfTriplesMapper.of(
                triplesMap, refObjectMappers, incomingRefObjectMappers, logicalSourceResolver, rdfMapperConfig);

        // Then
        assertThat(rdfTriplesMapper, is(not(nullValue())));
        assertThat(rdfTriplesMapper.getRefObjectMappers(), is(empty()));
        assertThat(rdfTriplesMapper.getConnectedRefObjectMappers(), is(Set.of(rdfRefObjectMapper1)));
        assertThat(rdfTriplesMapper.getTriplesMap(), is(triplesMap));
    }

    @Test
    void givenNoSubjectMap_whenOfCalled_thenThrowException() {
        // Given
        when(triplesMap.getSubjectMaps()).thenReturn(Set.of());
        when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
        when(triplesMap.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("triplesMap"));
        RdfMapperConfig rdfMapperConfig = mock(RdfMapperConfig.class);

        // When
        Throwable exception = assertThrows(
                TriplesMapperException.class,
                () -> RdfTriplesMapper.of(
                        triplesMap,
                        refObjectMappers,
                        incomingRefObjectMappers,
                        logicalSourceResolver,
                        rdfMapperConfig));

        // Then
        assertThat(
                exception.getMessage(),
                startsWith("Subject map must be specified in triples map blank node resource _:triplesMap in:"));
    }

    @Test
    void givenOnlySubjectMapWithClass_whenMap_thenReturnTypeStatement() {
        // Given
        IRI subject = VALUE_FACTORY.createIRI("http://foo.bar/subject");
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(subject));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        IRI class1 = VALUE_FACTORY.createIRI("http://foo.bar/class1");
        when(subjectMap.getClasses()).thenReturn(Set.of(class1));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> VALUE_FACTORY)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(
                triplesMap, refObjectMappers, incomingRefObjectMappers, logicalSourceResolver, rdfMapperConfig);

        // When
        Flux<Statement> statements = rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        StepVerifier.create(statements)
                .expectNext(VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1))
                .verifyComplete();
    }

    @Test
    void givenSubjectMapThatReturnsNothing_whenMap_thenReturnEmptyFlux() {
        // Given
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> VALUE_FACTORY)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(
                triplesMap, refObjectMappers, incomingRefObjectMappers, logicalSourceResolver, rdfMapperConfig);

        // When
        Flux<Statement> statements = rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        StepVerifier.create(statements).expectNextCount(0).verifyComplete();
    }

    @Test
    void givenSubjectMapAndPom_whenMap_thenReturnStatements() {
        // Given
        IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(subject1));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

        when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI subjectGraph1 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph1");
        when(graphGenerator1.apply(any(), any())).thenReturn(List.of(subjectGraph1, Rdf.Rr.defaultGraph));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(predicate1));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = VALUE_FACTORY.createLiteral("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(object1));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap2));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap2)).thenReturn(graphGenerator2);
        IRI graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph1");
        when(graphGenerator2.apply(any(), any())).thenReturn(List.of(graph1, Rdf.Rr.defaultGraph));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> VALUE_FACTORY)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(
                triplesMap, refObjectMappers, incomingRefObjectMappers, logicalSourceResolver, rdfMapperConfig);

        // When
        Flux<Statement> statements = rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        Predicate<Statement> expectedStatement = statement -> Set.of(
                        VALUE_FACTORY.createStatement(subject1, predicate1, object1, subjectGraph1),
                        VALUE_FACTORY.createStatement(subject1, predicate1, object1, graph1),
                        VALUE_FACTORY.createStatement(subject1, predicate1, object1))
                .contains(statement);

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

        IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(subject1));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

        IRI subject2 = VALUE_FACTORY.createIRI("http://foo.bar/subject2");
        when(subjectGenerator2.apply(any(), any())).thenReturn(List.of(subject2));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap2)).thenReturn(subjectGenerator2);

        when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI subjectGraph1 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph1");
        when(graphGenerator1.apply(any(), any())).thenReturn(List.of(subjectGraph1, Rdf.Rr.defaultGraph));

        when(subjectMap2.getGraphMaps()).thenReturn(Set.of(graphMap2));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap2)).thenReturn(graphGenerator2);
        IRI subjectGraph2 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph2");
        when(graphGenerator2.apply(any(), any())).thenReturn(List.of(subjectGraph2));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(predicate1));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = VALUE_FACTORY.createLiteral("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(object1));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap3));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap3)).thenReturn(graphGenerator3);
        IRI graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph1");
        when(graphGenerator3.apply(any(), any())).thenReturn(List.of(graph1));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> VALUE_FACTORY)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(
                triplesMap, refObjectMappers, incomingRefObjectMappers, logicalSourceResolver, rdfMapperConfig);

        // When
        Flux<Statement> statements = rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        Predicate<Statement> expectedStatement = statement -> Set.of(
                        VALUE_FACTORY.createStatement(subject1, predicate1, object1, subjectGraph1),
                        VALUE_FACTORY.createStatement(subject1, predicate1, object1, graph1),
                        VALUE_FACTORY.createStatement(subject1, predicate1, object1),
                        VALUE_FACTORY.createStatement(subject2, predicate1, object1, subjectGraph2),
                        VALUE_FACTORY.createStatement(subject2, predicate1, object1, graph1))
                .contains(statement);

        StepVerifier.create(statements)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void givenSubjectMapAndIncomingRefObjectMappers_whenMap_thenCacheParentSideJoinConditions() {
        // Given
        IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of(subject1));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

        when(rdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap1);
        when(refObjectMap1.getJoinConditions()).thenReturn(Set.of(join1));
        when(join1.getParentMap()).thenReturn(parentMap1);
        when(parentMap1.getReference()).thenReturn("bar1");

        when(rdfRefObjectMapper2.getRefObjectMap()).thenReturn(refObjectMap2);

        when(refObjectMap2.getJoinConditions()).thenReturn(Set.of(join2, join3));
        when(join2.getParentMap()).thenReturn(parentMap2);
        when(parentMap2.getReference()).thenReturn("bar2");
        when(join3.getParentMap()).thenReturn(parentMap3);
        when(parentMap3.getReference()).thenReturn("bar3");

        incomingRefObjectMappers = Set.of(rdfRefObjectMapper1, rdfRefObjectMapper2);

        when(logicalSourceResolver.getExpressionEvaluationFactory()).thenReturn(expressionEvaluationFactory);
        when(expressionEvaluationFactory.apply(any())).thenReturn(expressionEvaluation);
        when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> VALUE_FACTORY)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(
                triplesMap, refObjectMappers, incomingRefObjectMappers, logicalSourceResolver, rdfMapperConfig);

        // When
        Flux<Statement> statements = rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        StepVerifier.create(statements).verifyComplete();

        ParentSideJoinConditionStore<Resource> joinConditions = rdfTriplesMapper.getParentSideJoinConditions();

        assertThat(joinConditions.get(ParentSideJoinKey.of("bar1", "baz")), is(Set.of(subject1)));
        assertThat(joinConditions.get(ParentSideJoinKey.of("bar2", "baz")), is(Set.of(subject1)));
        assertThat(joinConditions.get(ParentSideJoinKey.of("bar3", "baz")), is(Set.of(subject1)));
    }

    @Test
    void
            givenIncomingRefObjectMapperWithSameParentKeyInMultipleMaps_whenMap_thenAddToExistingParentSideJoinConditions() {
        // Given
        IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
        IRI subject2 = VALUE_FACTORY.createIRI("http://foo.bar/subject2");
        IRI subject3 = VALUE_FACTORY.createIRI("http://foo.bar/subject3");

        when(subjectGenerator.apply(any(), any()))
                .thenReturn(List.of(subject1))
                .thenReturn(List.of(subject2))
                .thenReturn(List.of(subject3));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

        when(rdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap1);
        when(refObjectMap1.getJoinConditions()).thenReturn(Set.of(join1));
        when(join1.getParentMap()).thenReturn(parentMap1);
        when(parentMap1.getReference()).thenReturn("bar1");

        incomingRefObjectMappers = Set.of(rdfRefObjectMapper1);

        when(expressionEvaluationFactory.apply(any())).thenReturn(expressionEvaluation);
        when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> VALUE_FACTORY)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(
                triplesMap, refObjectMappers, incomingRefObjectMappers, logicalSourceResolver, rdfMapperConfig);

        // When
        rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        ParentSideJoinConditionStore<Resource> joinConditions = rdfTriplesMapper.getParentSideJoinConditions();
        assertThat(joinConditions.get(ParentSideJoinKey.of("bar1", "baz")), is(Set.of(subject1)));

        // When
        rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        assertThat(joinConditions.get(ParentSideJoinKey.of("bar1", "baz")), is(Set.of(subject1, subject2)));

        // When
        rdfTriplesMapper.map(logicalSourceRecord);

        // Then
        assertThat(joinConditions.get(ParentSideJoinKey.of("bar1", "baz")), is(Set.of(subject1, subject2, subject3)));
    }
}
