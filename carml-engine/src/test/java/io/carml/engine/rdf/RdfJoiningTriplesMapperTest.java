package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Statements.statement;
import static org.eclipse.rdf4j.model.util.Values.bnode;
import static org.eclipse.rdf4j.model.util.Values.getValueFactory;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.engine.join.ParentSideJoinConditionStoreProvider;
import io.carml.engine.join.impl.CarmlParentSideJoinConditionStoreProvider;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.model.GraphMap;
import io.carml.model.LogicalSource;
import io.carml.model.PredicateMap;
import io.carml.model.PredicateObjectMap;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.model.impl.CarmlSubjectMap;
import io.carml.vocab.Rdf.Rml;
import java.util.List;
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

@ExtendWith({MockitoExtension.class})
class RdfJoiningTriplesMapperTest {

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private TriplesMap triplesMap2;

    @Mock
    private TermGenerator<Resource> subjectGenerator;

    @Mock
    private TermGenerator<Resource> subjectGenerator2;

    @Mock
    private SubjectMap subjectMap;

    @Mock
    private TermGenerator<IRI> predicateGenerator1;

    @Mock
    private PredicateMap predicateMap1;

    @Mock
    private TermGenerator<Value> objectGenerator1;

    @Mock
    private RefObjectMap refObjectMap1;

    @Mock
    private PredicateObjectMap joiningPom;

    @Mock
    private TermGenerator<Resource> graphGenerator1;

    @Mock
    private GraphMap graphMap1;

    @Mock
    private TermGenerator<Resource> graphGenerator2;

    @Mock
    private GraphMap graphMap2;

    private Set<PredicateObjectMap> joiningPredicateObjectMaps;

    @Mock
    private LogicalSource virtualJoiningLogicalSource;

    @Mock
    private LogicalSourceResolver<String> logicalSourceResolver;

    @Mock
    private LogicalSourceResolver.ExpressionEvaluationFactory<String> expressionEvaluationFactory;

    @Mock
    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Mock
    private ChildSideJoinStoreProvider<MappedValue<Resource>, MappedValue<IRI>> childSideJoinStoreProvider;

    private ParentSideJoinConditionStoreProvider<MappedValue<Resource>> parentSideJoinConditionStoreProvider;

    @Mock
    private LogicalSourceRecord<?> logicalSourceRecord;

    @BeforeEach
    void setup() {
        lenient().when(logicalSourceResolver.getExpressionEvaluationFactory()).thenReturn(expressionEvaluationFactory);
        when(triplesMap.getSubjectMaps()).thenReturn(Set.of(subjectMap));
        lenient().when(triplesMap.getId()).thenReturn("triples-map-1");
        lenient().when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
        joiningPredicateObjectMaps = Set.of(joiningPom);
        parentSideJoinConditionStoreProvider = CarmlParentSideJoinConditionStoreProvider.of();
    }

    @Test
    void givenAllParams_whenOfCalled_thenReturnRdfJoiningTripleMapper() {
        // Given
        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        // When
        var rdfJoiningTriplesMapper = RdfJoiningTriplesMapper.of(
                triplesMap,
                joiningPredicateObjectMaps,
                virtualJoiningLogicalSource,
                logicalSourceResolver,
                rdfMapperConfig);

        // Then
        assertThat(rdfJoiningTriplesMapper, is(not(nullValue())));
        assertThat(rdfJoiningTriplesMapper.getLogicalSource(), is(virtualJoiningLogicalSource));
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
                () -> RdfJoiningTriplesMapper.of(
                        triplesMap,
                        joiningPredicateObjectMaps,
                        virtualJoiningLogicalSource,
                        logicalSourceResolver,
                        rdfMapperConfig));

        // Then
        assertThat(
                exception.getMessage(),
                startsWith("Subject map must be specified in triples map blank node resource _:triplesMap in:"));
    }

    @Test
    void givenSubjectMapThatReturnsNothing_whenMap_thenReturnEmptyFlux() {
        // Given
        when(subjectGenerator.apply(any(), any())).thenReturn(List.of());

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        RdfJoiningTriplesMapper<String> rdfJoiningTriplesMapper = RdfJoiningTriplesMapper.of(
                triplesMap,
                joiningPredicateObjectMaps,
                virtualJoiningLogicalSource,
                logicalSourceResolver,
                rdfMapperConfig);

        // When
        var statements = rdfJoiningTriplesMapper.map(logicalSourceRecord);

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

        when(joiningPom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
        when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
        IRI predicate1 = iri("http://foo.bar/predicate1");
        when(predicateGenerator1.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(predicate1)));

        when(joiningPom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));
        when(refObjectMap1.getParentTriplesMap()).thenReturn(triplesMap2);

        var subjectMap2 = CarmlSubjectMap.builder().build();

        when(triplesMap2.getSubjectMaps()).thenReturn(Set.of(subjectMap2));
        when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap2)).thenReturn(subjectGenerator2);
        Resource object1 = iri("http://foo.bar/object1");
        when(subjectGenerator2.apply(any(), any())).thenReturn(List.of(RdfMappedValue.of(object1)));

        when(joiningPom.getGraphMaps()).thenReturn(Set.of(graphMap2));
        when(rdfTermGeneratorFactory.getGraphGenerator(graphMap2)).thenReturn(graphGenerator2);
        IRI graph1 = iri("http://foo.bar/graph1");
        when(graphGenerator2.apply(any(), any()))
                .thenReturn(List.of(RdfMappedValue.of(graph1), RdfMappedValue.of(Rml.defaultGraph)));

        var rdfMapperConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(() -> getValueFactory())
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                .build();

        var rdfJoiningTriplesMapper = RdfJoiningTriplesMapper.of(
                triplesMap,
                joiningPredicateObjectMaps,
                virtualJoiningLogicalSource,
                logicalSourceResolver,
                rdfMapperConfig);

        // When
        var statements = rdfJoiningTriplesMapper.map(logicalSourceRecord);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedTriple -> Set.of(
                        statement(subject1, predicate1, object1, subjectGraph1),
                        statement(subject1, predicate1, object1, graph1),
                        statement(subject1, predicate1, object1, null))
                .contains(Mono.from(mappedTriple.getResults()).block());

        StepVerifier.create(statements)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }
}
