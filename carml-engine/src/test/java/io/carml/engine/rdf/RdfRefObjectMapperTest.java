package io.carml.engine.rdf;

import static org.eclipse.rdf4j.model.util.Statements.statement;
import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import io.carml.engine.MappedValue;
import io.carml.engine.MappingResult;
import io.carml.engine.join.ChildSideJoin;
import io.carml.engine.join.ChildSideJoinCondition;
import io.carml.engine.join.ChildSideJoinStore;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.engine.join.ParentSideJoinKey;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.ChildMap;
import io.carml.model.Join;
import io.carml.model.ParentMap;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfRefObjectMapperTest {

    @Mock
    private TriplesMap triplesMap;

    @Mock
    private RefObjectMap refObjectMap;

    @Mock
    private Join join1;

    @Mock
    private ChildMap childMap1;

    @Mock
    private ParentMap parentMap1;

    @Mock
    private Join join2;

    @Mock
    private ChildMap childMap2;

    @Mock
    private ParentMap parentMap2;

    @Mock
    private RdfTermGeneratorFactory rdfTermGeneratorFactory;

    @Mock
    private ChildSideJoinStoreProvider<MappedValue<Resource>, MappedValue<IRI>> childSideJoinStoreProvider;

    @Mock
    private ExpressionEvaluation expressionEvaluation;

    @Mock
    private ChildSideJoinStore<MappedValue<Resource>, MappedValue<IRI>> childSideJoinStore;

    @Mock
    private RdfTriplesMapper<?> parentRdfTriplesMapper;

    @Mock
    private ParentSideJoinConditionStore<MappedValue<Resource>> parentSideJoinConditionStore;

    @Captor
    private ArgumentCaptor<Set<ChildSideJoin<MappedValue<Resource>, MappedValue<IRI>>>> childSideJoinCaptor;

    @BeforeEach
    void setup() {
        when(childSideJoinStoreProvider.createChildSideJoinStore(any())).thenReturn(childSideJoinStore);
    }

    @Test
    void givenAllParams_whenOfCalled_thenConstructRdfRefObjectMapper() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        // When
        RdfRefObjectMapper rdfRefObjectMapper = RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingConfig);

        // Then
        assertThat(rdfRefObjectMapper, is(not(nullValue())));
        assertThat(rdfRefObjectMapper.getRefObjectMap(), is(refObjectMap));
    }

    @Test
    void givenRefObjectMapperWithAllArgsWithSingleJoinCondition_whenMap_thenChildSideJoinConditionWithConditionAdded() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1));
        when(join1.getChildMap()).thenReturn(childMap1);
        when(childMap1.getReference()).thenReturn("foo");
        when(join1.getParentMap()).thenReturn(parentMap1);
        when(parentMap1.getReference()).thenReturn("bar");

        when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

        Set<MappedValue<Resource>> subjects = Set.of(MappedValue.of(iri("http://foo.bar/subject1")));
        Set<MappedValue<Resource>> graphs = Set.of(MappedValue.of(iri("http://foo.bar/graph1")));

        Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndAllGraphs = Map.of(subjects, graphs);

        RdfRefObjectMapper rdfRefObjectMapper = RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingConfig);

        Set<MappedValue<IRI>> predicates = Set.of(MappedValue.of(iri("http://foo.bar/predicate1")));

        // When
        Mono<Statement> refObjectMapperPromise = Mono.empty();
        rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

        // Then
        StepVerifier.create(refObjectMapperPromise).expectComplete().verify();

        verify(childSideJoinStore, times(1)).addAll(childSideJoinCaptor.capture());

        var childSideJoins = childSideJoinCaptor.getValue();

        var childSideJoin = Iterables.getOnlyElement(childSideJoins);

        assertThat(childSideJoin.getSubjects(), is(subjects));
        assertThat(childSideJoin.getPredicates(), is(predicates));
        assertThat(childSideJoin.getGraphs(), is(graphs));

        Set<ChildSideJoinCondition> childSideJoinConditions = childSideJoin.getChildSideJoinConditions();

        assertThat(childSideJoinConditions, hasSize(1));

        ChildSideJoinCondition childSideJoinCondition = Iterables.getOnlyElement(childSideJoinConditions);

        assertThat(childSideJoinCondition.getChildReference(), is("foo"));
        assertThat(childSideJoinCondition.getChildValues(), containsInAnyOrder("baz"));
        assertThat(childSideJoinCondition.getParentReference(), is("bar"));
    }

    @Test
    void givenRefObjectMapperWitMultipleSubjectAndGraphCombos_whenMap_thenChildSideJoinConditionWithConditionAdded() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1));
        when(join1.getChildMap()).thenReturn(childMap1);
        when(childMap1.getReference()).thenReturn("foo");
        when(join1.getParentMap()).thenReturn(parentMap1);
        when(parentMap1.getReference()).thenReturn("bar");

        Resource subject1 = iri("http://foo.bar/subject1");
        MappedValue<Resource> mappedSubject1 = MappedValue.of(subject1);
        Resource subject2 = iri("http://foo.bar/subject2");
        MappedValue<Resource> mappedSubject2 = MappedValue.of(subject2);
        Resource graph1 = iri("http://foo.bar/graph1");
        MappedValue<Resource> mappedGraph1 = MappedValue.of(graph1);
        Resource graph2 = iri("http://foo.bar/graph2");
        MappedValue<Resource> mappedGraph2 = MappedValue.of(graph2);
        Set<MappedValue<Resource>> subjects = Set.of(mappedSubject1);
        Set<MappedValue<Resource>> subjects2 = Set.of(mappedSubject2);
        Set<MappedValue<Resource>> graphs = Set.of(mappedGraph1);
        Set<MappedValue<Resource>> graphs2 = Set.of(mappedGraph2);

        when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

        var subjectsAndAllGraphs = Map.of(subjects, graphs, subjects2, graphs2);

        RdfRefObjectMapper rdfRefObjectMapper = RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingConfig);

        IRI predicate1 = iri("http://foo.bar/predicate1");
        var mappedPredicate1 = MappedValue.of(predicate1);

        Set<MappedValue<IRI>> predicates = Set.of(mappedPredicate1);

        // When
        var refObjectMapperPromise = Mono.empty();
        rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

        // Then
        StepVerifier.create(refObjectMapperPromise).expectComplete().verify();

        verify(childSideJoinStore, times(1)).addAll(childSideJoinCaptor.capture());

        var childSideJoins = childSideJoinCaptor.getValue();

        assertThat(
                childSideJoins.stream()
                        .map(ChildSideJoin::getSubjects)
                        .flatMap(Set::stream)
                        .collect(Collectors.toUnmodifiableSet()),
                containsInAnyOrder(mappedSubject1, mappedSubject2));
        assertThat(
                childSideJoins.stream()
                        .map(ChildSideJoin::getPredicates)
                        .flatMap(Set::stream)
                        .collect(Collectors.toUnmodifiableSet()),
                containsInAnyOrder(mappedPredicate1));
        assertThat(
                childSideJoins.stream()
                        .map(ChildSideJoin::getGraphs)
                        .flatMap(Set::stream)
                        .collect(Collectors.toUnmodifiableSet()),
                containsInAnyOrder(mappedGraph1, mappedGraph2));
    }

    @Test
    void givenRefObjectMapperWithMultipleJoinConditions_whenMap_thenChildSideJoinConditionWithAllConditionsAdded() {
        // Given
        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1, join2));
        when(join1.getChildMap()).thenReturn(childMap1);
        when(childMap1.getReference()).thenReturn("foo");

        when(join1.getParentMap()).thenReturn(parentMap1);
        when(parentMap1.getReference()).thenReturn("bar");

        when(join2.getChildMap()).thenReturn(childMap2);
        when(childMap2.getReference()).thenReturn("Alice");
        when(join2.getParentMap()).thenReturn(parentMap2);
        when(parentMap2.getReference()).thenReturn("Bob");

        when(expressionEvaluation.apply(any()))
                .thenReturn(Optional.of(List.of("baz")))
                .thenReturn(Optional.of(List.of("Carol")));

        Set<MappedValue<Resource>> subjects = Set.of(MappedValue.of(iri("http://foo.bar/subject1")));
        Set<MappedValue<Resource>> graphs = Set.of(MappedValue.of(iri("http://foo.bar/graph1")));

        Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndAllGraphs = Map.of(subjects, graphs);

        Set<MappedValue<IRI>> predicates = Set.of(MappedValue.of(iri("http://foo.bar/predicate1")));

        RdfRefObjectMapper rdfRefObjectMapper = RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingConfig);

        // When
        Mono<Statement> refObjectMapperPromise = Mono.empty();
        rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

        // Then
        StepVerifier.create(refObjectMapperPromise).verifyComplete();

        verify(childSideJoinStore, times(1)).addAll(childSideJoinCaptor.capture());

        var childSideJoins = childSideJoinCaptor.getValue();

        var childSideJoin = Iterables.getOnlyElement(childSideJoins);

        assertThat(childSideJoin.getSubjects(), is(subjects));
        assertThat(childSideJoin.getPredicates(), is(predicates));
        assertThat(childSideJoin.getGraphs(), is(graphs));

        Set<ChildSideJoinCondition> childSideJoinConditions = childSideJoin.getChildSideJoinConditions();

        assertThat(childSideJoinConditions, hasSize(2));
    }

    @Test
    void givenValidJoinWithTwoParentValues_whenResolveJoins_ThenReturnsTwoStatements() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        Set<MappedValue<Resource>> subjects = Set.of(MappedValue.of(subject1));
        IRI predicate1 = iri("http://foo.bar/predicate1");
        Set<MappedValue<IRI>> predicates = Set.of(MappedValue.of(predicate1));
        IRI graph1 = iri("http://foo.bar/graph1");
        Set<MappedValue<Resource>> graphs = Set.of(MappedValue.of(graph1));

        var childSideJoin1 = ChildSideJoin.<MappedValue<Resource>, MappedValue<IRI>>builder()
                .subjects(new HashSet<>(subjects))
                .predicates(new HashSet<>(predicates))
                .graphs(new HashSet<>(graphs))
                .childSideJoinConditions(
                        new HashSet<>(Set.of(ChildSideJoinCondition.of("foo", new ArrayList<>(List.of("baz")), "bar"))))
                .build();

        when(childSideJoinStore.clearingFlux()).thenReturn(Flux.just(childSideJoin1));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfRefObjectMapper rdfRefObjectMapper = RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingConfig);

        IRI parentSubject1 = iri("http://foo.bar/parentSubject1");
        IRI parentSubject2 = iri("http://foo.bar/parentSubject2");
        Set<MappedValue<Resource>> parentSubjects =
                Set.of(MappedValue.of(parentSubject1), MappedValue.of(parentSubject2));

        ParentSideJoinKey parentSideJoinKey = ParentSideJoinKey.of("bar", "baz");

        when(parentSideJoinConditionStore.containsKey(parentSideJoinKey)).thenReturn(true);
        when(parentSideJoinConditionStore.get(parentSideJoinKey)).thenReturn(parentSubjects);

        when(parentRdfTriplesMapper.getParentSideJoinConditions()).thenReturn(parentSideJoinConditionStore);

        // When
        var joinedStatementFlux = rdfRefObjectMapper.resolveJoins(parentRdfTriplesMapper);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Set.of(
                        statement(subject1, predicate1, parentSubject1, graph1),
                        statement(subject1, predicate1, parentSubject2, graph1))
                .contains(Mono.from(mappedStatement.getResults()).block());

        StepVerifier.create(joinedStatementFlux)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }

    @Test
    void givenValidJoinWithTwoParentValuesForTwoSubjectGraphCombos_whenResolveJoins_ThenReturnsFourStatements() {
        // Given
        IRI subject1 = iri("http://foo.bar/subject1");
        Set<MappedValue<Resource>> subjects = Set.of(MappedValue.of(subject1));
        IRI predicate1 = iri("http://foo.bar/predicate1");
        Set<MappedValue<IRI>> predicates = Set.of(MappedValue.of(predicate1));
        IRI graph1 = iri("http://foo.bar/graph1");
        Set<MappedValue<Resource>> graphs = Set.of(MappedValue.of(graph1));

        IRI subject2 = iri("http://foo.bar/subject1");
        Set<MappedValue<Resource>> subjects2 = Set.of(MappedValue.of(subject2));
        IRI graph2 = iri("http://foo.bar/graph2");
        Set<MappedValue<Resource>> graphs2 = Set.of(MappedValue.of(graph2));

        var childSideJoin1 = ChildSideJoin.<MappedValue<Resource>, MappedValue<IRI>>builder()
                .subjects(new HashSet<>(subjects))
                .predicates(new HashSet<>(predicates))
                .graphs(new HashSet<>(graphs))
                .childSideJoinConditions(
                        new HashSet<>(Set.of(ChildSideJoinCondition.of("foo", new ArrayList<>(List.of("baz")), "bar"))))
                .build();

        var childSideJoin2 = ChildSideJoin.<MappedValue<Resource>, MappedValue<IRI>>builder()
                .subjects(new HashSet<>(subjects2))
                .predicates(new HashSet<>(predicates))
                .graphs(new HashSet<>(graphs2))
                .childSideJoinConditions(
                        new HashSet<>(Set.of(ChildSideJoinCondition.of("foo", new ArrayList<>(List.of("baz")), "bar"))))
                .build();

        when(childSideJoinStore.clearingFlux()).thenReturn(Flux.just(childSideJoin1, childSideJoin2));

        RdfMapperConfig rdfMappingConfig = RdfMapperConfig.builder()
                .valueFactorySupplier(Values::getValueFactory)
                .termGeneratorFactory(rdfTermGeneratorFactory)
                .childSideJoinStoreProvider(childSideJoinStoreProvider)
                .build();

        RdfRefObjectMapper rdfRefObjectMapper = RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingConfig);

        IRI parentSubject1 = iri("http://foo.bar/parentSubject1");
        IRI parentSubject2 = iri("http://foo.bar/parentSubject2");
        Set<MappedValue<Resource>> parentSubjects =
                Set.of(MappedValue.of(parentSubject1), MappedValue.of(parentSubject2));

        ParentSideJoinKey parentSideJoinKey = ParentSideJoinKey.of("bar", "baz");

        when(parentSideJoinConditionStore.containsKey(parentSideJoinKey)).thenReturn(true);
        when(parentSideJoinConditionStore.get(parentSideJoinKey)).thenReturn(parentSubjects);

        when(parentRdfTriplesMapper.getParentSideJoinConditions()).thenReturn(parentSideJoinConditionStore);

        // When
        var joinedStatementFlux = rdfRefObjectMapper.resolveJoins(parentRdfTriplesMapper);

        // Then
        Predicate<MappingResult<Statement>> expectedStatement = mappedStatement -> Set.of(
                        statement(subject1, predicate1, parentSubject1, graph1),
                        statement(subject1, predicate1, parentSubject2, graph1),
                        statement(subject2, predicate1, parentSubject1, graph2),
                        statement(subject2, predicate1, parentSubject2, graph2))
                .contains(Mono.from(mappedStatement.getResults()).block());

        StepVerifier.create(joinedStatementFlux)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .expectNextMatches(expectedStatement)
                .verifyComplete();
    }
}
