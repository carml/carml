package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Statements.statement;
import static org.eclipse.rdf4j.model.util.Values.bnode;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.eclipse.rdf4j.model.util.Values.literal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.GraphMap;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.ObjectMap;
import io.carml.model.PredicateMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf.Rml;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfPredicateObjectMapperTest {

    @Mock
    private PredicateObjectMap pom;

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private LogicalSource logicalSource;

    @Mock
    private ChildSideJoinStoreProvider<MappedValue<Resource>, MappedValue<IRI>> childSideJoinStoreProvider;

    @Mock
    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Mock
    private Set<MappedValue<Resource>> subjects;

    @Mock
    private Set<MappedValue<Resource>> subjectGraphs;

    @Mock
    private TermGenerator<IRI> predicateGenerator1;

    @Mock
    private PredicateMap predicateMap1;

    @Mock
    private TermGenerator<Value> objectGenerator1;

    @Mock
    private ObjectMap objectMap1;

    @Mock
    private TermGenerator<Resource> graphGenerator1;

    @Mock
    private GraphMap graphMap1;

    @Mock
    private RefObjectMap refObjectMap1;

    @Mock
    private Join join1;

    @Mock
    private TriplesMap triplesMap2;

    @Mock
    private LogicalSource logicalSource2;

    @Mock
    private SubjectMap subjectMap2;

    @Mock
    private TermGenerator<Resource> subjectGenerator2;

    private Set<RdfRefObjectMapper> rdfRefObjectMappers;

    @Mock
    private RdfRefObjectMapper rdfRefObjectMapper1;

    @BeforeEach
    void setup() {
        rdfRefObjectMappers = new HashSet<>();
    }

    @Test
    void givenAllParams_whenOfCalled_thenConstructRdfPredicateObjectMapper() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        // When
        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingConfig);

        // Then
        assertThat(rdfPredicateObjectMapper, is(not(nullValue())));
        assertThat(rdfPredicateObjectMapper.getRdfRefObjectMappers(), is(empty()));
    }

    @Test
    void givenAllParams_whenForTableJoiningCalled_thenConstructRdfPredicateObjectMapper() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        // When
        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.forTableJoining(pom, triplesMap, rdfMappingConfig, "foo");

        // Then
        assertThat(rdfPredicateObjectMapper, is(not(nullValue())));
        assertThat(rdfPredicateObjectMapper.getRdfRefObjectMappers(), is(empty()));
    }

    @Test
    void givenSingleJoinlessRefObjectMapWithDifferentLogicalSource_whenOfCalled_thenThrowException() {
        // Given
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
        when(logicalSource.getAsResource()).thenReturn(bnode("logicalSource"));

        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);

        when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));
        when(refObjectMap1.getJoinConditions()).thenReturn(Set.of());
        when(refObjectMap1.getParentTriplesMap()).thenReturn(triplesMap2);
        when(refObjectMap1.getAsResource()).thenReturn(bnode("refObjectMap1"));

        when(triplesMap2.getLogicalSource()).thenReturn(logicalSource2);
        when(triplesMap2.asRdf()).thenReturn(new ModelBuilder().build());
        when(logicalSource2.getAsResource()).thenReturn(bnode("logicalSource2"));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        // When
        Throwable exception = assertThrows(
                TriplesMapperException.class,
                () -> RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingConfig));

        // Then
        assertThat(exception.getMessage(), startsWith("Logical sources are not equal."));
    }

    @Test
    void givenPredicateGeneratorReturningEmpty_whenMap_thenReturnNothing() {
        // Given
        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of());

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then
        StepVerifier.create(pomStatements).verifyComplete();
    }

    @Test
    void givenSingleValuedSubPredObjGenerators_whenMap_thenGenerateSingleStatement() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = literal("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Objects.equals(
                statement(subject1, predicate1, object1, null),
                Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(pomStatements).expectNextMatches(expectedStatement).verifyComplete();
    }

    @Test
    void givenSubjectGraphsAndGraphGenerators_whenMap_generatesStatementsForAllGraphs() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = literal("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI graph1 = iri("http://foo.bar/graph");
        when(graphGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(graph1)));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Set.of(
                        statement(subject1, predicate1, object1, subjectGraph1),
                        statement(subject1, predicate1, object1, graph1))
                .contains(Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(pomStatements)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void
            givenSubjectGraphsWithDefaultGraphAndGraphGeneratorsWithDefaultGraph_whenMap_generatesStatementsForAllGraphs() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
        when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
        Value object1 = literal("object1");
        when(objectGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1), RdfMappedValue.of(Rml.defaultGraph));

        when(pom.getGraphMaps()).thenReturn(Set.of(graphMap1));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
        IRI graph1 = iri("http://foo.bar/graph1");
        when(graphGenerator1.apply(any(), any()))
                .thenReturn(List.of(RdfMappedValue.of(graph1), RdfMappedValue.of(Rml.defaultGraph)));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Set.of(
                        statement(subject1, predicate1, object1, subjectGraph1),
                        statement(subject1, predicate1, object1, graph1),
                        statement(subject1, predicate1, object1, null))
                .contains(Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(pomStatements)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void givenSingleJoinlessRefObjectMap_whenMap_thenReturnJoinedStatementDirectly() {
        // Given
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);

        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));
        when(refObjectMap1.getJoinConditions()).thenReturn(Set.of());
        when(refObjectMap1.getParentTriplesMap()).thenReturn(triplesMap2);

        when(triplesMap2.getLogicalSource()).thenReturn(logicalSource);
        when(triplesMap2.getSubjectMaps()).thenReturn(Set.of(subjectMap2));

        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap2)).thenReturn(subjectGenerator2);

        IRI subject2 = iri("http://foo.bar/subject2");
        when(subjectGenerator2.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(subject2)));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingConfig);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(any(), any(), subjectsAndSubjectGraphs);

        // Then

        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Objects.equals(
                statement(subject1, predicate1, subject2, subjectGraph1),
                Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(pomStatements).expectNextMatches(expectedStatement).verifyComplete();
    }

    @Test
    void givenSingleJoinSingleConditionedRefObjectMap_whenMap_thenChildSideJoinConditionCached() {
        // Given
        when(triplesMap.getLogicalSource()).thenReturn(logicalSource);

        IRI subject1 = iri("http://foo.bar/subject1");
        subjects = Set.of(RdfMappedValue.of(subject1));

        when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));

        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(refObjectMap1.getJoinConditions()).thenReturn(Set.of(join1));

        IRI subjectGraph1 = iri("http://foo.bar/subjectGraph1");
        subjectGraphs = Set.of(RdfMappedValue.of(subjectGraph1));

        rdfRefObjectMappers = Set.of(rdfRefObjectMapper1);
        when(rdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap1);

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfPredicateObjectMapper rdfPredicateObjectMapper =
                RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingConfig);

        ExpressionEvaluation expressionEvaluation = mock(ExpressionEvaluation.class);

        var subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

        // When
        var pomStatements = rdfPredicateObjectMapper.map(expressionEvaluation, null, subjectsAndSubjectGraphs);

        // Then
        verify(rdfRefObjectMapper1, times(1))
                .map(subjectsAndSubjectGraphs, Set.of(RdfMappedValue.of(predicate1)), expressionEvaluation);

        StepVerifier.create(pomStatements).verifyComplete();
    }
}
