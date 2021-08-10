package com.taxonic.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoin;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinCondition;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinKey;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfRefObjectMapperTest {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  @Mock
  private TriplesMap triplesMap;

  @Mock
  private RefObjectMap refObjectMap;

  @Mock
  private Join join1;

  @Mock
  private Join join2;

  @Mock
  private RdfTermGeneratorFactory rdfTermGeneratorFactory;

  @Mock
  private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider;

  @Mock
  private ExpressionEvaluation expressionEvaluation;

  @Mock
  private Set<ChildSideJoin<Resource, IRI>> childSideJoinCache;

  @Mock
  private RdfTriplesMapper<?> parentRdfTriplesMapper;

  @Mock
  Mono<Void> completion;

  @Captor
  private ArgumentCaptor<ChildSideJoin<Resource, IRI>> childSideJoinCaptor;

  @BeforeEach
  void setup() {
    when(childSideJoinStoreProvider.create(any())).thenReturn(childSideJoinCache);
  }

  @Test
  void givenAllParams_whenOfCalled_thenConstructRdfRefObjectMapper() {
    // Given
    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    // When
    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    // Then
    assertThat(rdfRefObjectMapper, is(not(nullValue())));
    assertThat(rdfRefObjectMapper.getRefObjectMap(), is(refObjectMap));
  }

  @Test
  void givenRefObjectMapperWithAllArgsWithSingleJoinCondition_whenMap_thenChildSideJoinConditionWithConditionAdded() {
    // Given
    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    Set<Resource> subjects = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/subject1"));
    Set<IRI> predicates = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/predicate1"));
    Set<Resource> graphs = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/graph1"));

    when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1));
    when(join1.getChildReference()).thenReturn("foo");
    when(join1.getParentReference()).thenReturn("bar");

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

    Map<Set<Resource>, Set<Resource>> subjectsAndAllGraphs = Map.of(subjects, graphs);

    // When
    Mono<Statement> refObjectMapperPromise = Mono.empty();
    rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

    // Then
    StepVerifier.create(refObjectMapperPromise)
        .expectComplete()
        .verify();

    verify(childSideJoinCache, times(1)).add(childSideJoinCaptor.capture());

    ChildSideJoin<Resource, IRI> childSideJoin = childSideJoinCaptor.getValue();

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
    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    Resource subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    Resource subject2 = VALUE_FACTORY.createIRI("http://foo.bar/subject2");
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    Resource graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph1");
    Resource graph2 = VALUE_FACTORY.createIRI("http://foo.bar/graph2");
    Set<Resource> subjects = Set.of(subject1);
    Set<Resource> subjects2 = Set.of(subject2);
    Set<IRI> predicates = Set.of(predicate1);
    Set<Resource> graphs = Set.of(graph1);
    Set<Resource> graphs2 = Set.of(graph2);

    when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1));
    when(join1.getChildReference()).thenReturn("foo");
    when(join1.getParentReference()).thenReturn("bar");

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

    Map<Set<Resource>, Set<Resource>> subjectsAndAllGraphs = Map.of(subjects, graphs, subjects2, graphs2);

    // When
    Mono<Statement> refObjectMapperPromise = Mono.empty();
    rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

    // Then
    StepVerifier.create(refObjectMapperPromise)
        .expectComplete()
        .verify();

    verify(childSideJoinCache, times(2)).add(childSideJoinCaptor.capture());

    List<ChildSideJoin<Resource, IRI>> childSideJoins = childSideJoinCaptor.getAllValues();

    assertThat(childSideJoins.stream()
        .map(ChildSideJoin::getSubjects)
        .flatMap(Set::stream)
        .collect(Collectors.toUnmodifiableSet()), containsInAnyOrder(subject1, subject2));
    assertThat(childSideJoins.stream()
        .map(ChildSideJoin::getPredicates)
        .flatMap(Set::stream)
        .collect(Collectors.toUnmodifiableSet()), containsInAnyOrder(predicate1));
    assertThat(childSideJoins.stream()
        .map(ChildSideJoin::getGraphs)
        .flatMap(Set::stream)
        .collect(Collectors.toUnmodifiableSet()), containsInAnyOrder(graph1, graph2));
  }

  @Test
  void givenRefObjectMapperWithMultipleJoinConditions_whenMap_thenChildSideJoinConditionWithAllConditionsAdded() {
    // Given
    Set<Resource> subjects = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/subject1"));
    Set<IRI> predicates = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/predicate1"));
    Set<Resource> graphs = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/graph1"));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1, join2));
    when(join1.getChildReference()).thenReturn("foo");
    when(join1.getParentReference()).thenReturn("bar");

    when(join2.getChildReference()).thenReturn("Alice");
    when(join2.getParentReference()).thenReturn("Bob");

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")))
        .thenReturn(Optional.of(List.of("Carol")));

    Map<Set<Resource>, Set<Resource>> subjectsAndAllGraphs = Map.of(subjects, graphs);

    // When
    Mono<Statement> refObjectMapperPromise = Mono.empty();
    rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

    // Then
    StepVerifier.create(refObjectMapperPromise)
        .verifyComplete();

    verify(childSideJoinCache, times(1)).add(childSideJoinCaptor.capture());

    ChildSideJoin<Resource, IRI> childSideJoin = childSideJoinCaptor.getValue();

    assertThat(childSideJoin.getSubjects(), is(subjects));
    assertThat(childSideJoin.getPredicates(), is(predicates));
    assertThat(childSideJoin.getGraphs(), is(graphs));

    Set<ChildSideJoinCondition> childSideJoinConditions = childSideJoin.getChildSideJoinConditions();

    assertThat(childSideJoinConditions, hasSize(2));
  }

  @Test
  void givenOnlyOneTriplesMapperDone_whenArrives_thenDeferPublishing() {
    // Given
    Set<Resource> subjects = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/subject1"));
    Set<IRI> predicates = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/predicate1"));
    Set<Resource> graphs = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/graph1"));

    ChildSideJoin<Resource, IRI> childSideJoin1 = ChildSideJoin.<Resource, IRI>builder()
        .subjects(subjects)
        .predicates(predicates)
        .graphs(graphs)
        .childSideJoinConditions(Set.of(ChildSideJoinCondition.of("foo", List.of("baz"), "bar")))
        .build();

    Set<ChildSideJoin<Resource, IRI>> childSideJoins = Set.of(childSideJoin1);

    when(childSideJoinStoreProvider.create(any())).thenReturn(childSideJoins);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    Flux<Statement> joinlessFlux = generateStatementsFor("main", 3);
    Flux<Statement> parentJoinlessFlux = generateStatementsFor("parent", 3).delayElements(Duration.ofSeconds(10));

    Flux<Statement> joinedStatementFlux =
        rdfRefObjectMapper.resolveJoins(joinlessFlux, parentRdfTriplesMapper, parentJoinlessFlux);

    // With deferred expectation
    StepVerifier deferred = StepVerifier.create(joinedStatementFlux)
        .expectComplete()
        .verifyLater();

    // When
    StepVerifier.create(joinlessFlux)
        .expectComplete();

    // Then
    Duration afterFourSeconds = Duration.ofSeconds(4);
    AssertionError timeOutAssertion = assertThrows(AssertionError.class, () -> deferred.verify(afterFourSeconds));
    assertThat(timeOutAssertion.getMessage(), containsString("timed out"));
  }

  @Test
  void givenTriplesMapperAndParentTriplesMapperDone_whenArrive_thenStartPublishing() {
    // Given
    Set<Resource> subjects = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/subject1"));
    Set<IRI> predicates = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/predicate1"));
    Set<Resource> graphs = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/graph1"));

    ChildSideJoin<Resource, IRI> childSideJoin1 = ChildSideJoin.<Resource, IRI>builder()
        .subjects(subjects)
        .predicates(predicates)
        .graphs(graphs)
        .childSideJoinConditions(Set.of(ChildSideJoinCondition.of("foo", List.of("baz"), "bar")))
        .build();

    Set<ChildSideJoin<Resource, IRI>> childSideJoins = new HashSet<>();
    childSideJoins.add(childSideJoin1);

    when(childSideJoinStoreProvider.create(any())).thenReturn(childSideJoins);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    when(parentRdfTriplesMapper.getParentSideJoinConditions()).thenReturn(new ConcurrentHashMap<>());

    when(parentRdfTriplesMapper.notifyCompletion(rdfRefObjectMapper, SignalType.ON_COMPLETE)).thenReturn(completion);

    Flux<Statement> joinlessFlux = generateStatementsFor("main", 3);
    Flux<Statement> parentJoinlessFlux = generateStatementsFor("parent", 3);

    Flux<Statement> joinedStatementFlux =
        rdfRefObjectMapper.resolveJoins(joinlessFlux, parentRdfTriplesMapper, parentJoinlessFlux);

    // With deferred expectation
    StepVerifier deferred = StepVerifier.create(joinedStatementFlux)
        .expectComplete()
        .verifyLater();

    // When
    StepVerifier.create(joinlessFlux)
        .expectComplete();

    StepVerifier.create(parentJoinlessFlux)
        .expectComplete();

    // Then
    deferred.verify(Duration.ofSeconds(1));

    StepVerifier.create(completion)
        .expectSubscription();
  }

  @Test
  void givenValidJoinWithTwoParentValues_whenResolveJoins_ThenReturnsTwoStatements() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    Set<Resource> subjects = Set.of(subject1);
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    Set<IRI> predicates = Set.of(predicate1);
    IRI graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph1");
    Set<Resource> graphs = Set.of(graph1);

    ChildSideJoin<Resource, IRI> childSideJoin1 = ChildSideJoin.<Resource, IRI>builder()
        .subjects(subjects)
        .predicates(predicates)
        .graphs(graphs)
        .childSideJoinConditions(Set.of(ChildSideJoinCondition.of("foo", List.of("baz"), "bar")))
        .build();

    Set<ChildSideJoin<Resource, IRI>> childSideJoins = new HashSet<>();
    childSideJoins.add(childSideJoin1);

    when(childSideJoinStoreProvider.create(any())).thenReturn(childSideJoins);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    IRI parentSubject1 = VALUE_FACTORY.createIRI("http://foo.bar/parentSubject1");
    IRI parentSubject2 = VALUE_FACTORY.createIRI("http://foo.bar/parentSubject2");
    Set<Resource> parentSubjects = Set.of(parentSubject1, parentSubject2);

    ParentSideJoinKey parentSideJoinKey = ParentSideJoinKey.of("bar", "baz");

    ConcurrentMap<ParentSideJoinKey, Set<Resource>> parentSideJoinConditions =
        new ConcurrentHashMap<>(Map.of(parentSideJoinKey, parentSubjects));

    when(parentRdfTriplesMapper.getParentSideJoinConditions()).thenReturn(parentSideJoinConditions);

    lenient().when(parentRdfTriplesMapper.notifyCompletion(any(), any()))
        .thenReturn(Mono.when());

    Flux<Statement> joinlessFlux = generateStatementsFor("main", 2);
    Flux<Statement> parentJoinlessFlux = generateStatementsFor("parent", 2);

    Flux<Statement> joinedStatementFlux =
        rdfRefObjectMapper.resolveJoins(joinlessFlux, parentRdfTriplesMapper, parentJoinlessFlux);

    // With deferred expectation
    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject1, predicate1, parentSubject1, graph1),
            VALUE_FACTORY.createStatement(subject1, predicate1, parentSubject2, graph1))
        .contains(statement);
    StepVerifier deferred = StepVerifier.create(joinedStatementFlux)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .expectComplete()
        .verifyLater();

    // When
    StepVerifier.create(joinlessFlux)
        .expectComplete();

    StepVerifier.create(parentJoinlessFlux)
        .expectComplete();

    // Then
    deferred.verify(Duration.ofSeconds(1));
  }

  @Test
  void givenValidJoinWithTwoParentValuesForTwoSubjectGraphCombos_whenResolveJoins_ThenReturnsFourStatements() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    Set<Resource> subjects = Set.of(subject1);
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    Set<IRI> predicates = Set.of(predicate1);
    IRI graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph1");
    Set<Resource> graphs = Set.of(graph1);


    IRI subject2 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    Set<Resource> subjects2 = Set.of(subject2);
    IRI graph2 = VALUE_FACTORY.createIRI("http://foo.bar/graph2");
    Set<Resource> graphs2 = Set.of(graph2);

    ChildSideJoin<Resource, IRI> childSideJoin1 = ChildSideJoin.<Resource, IRI>builder()
        .subjects(subjects)
        .predicates(predicates)
        .graphs(graphs)
        .childSideJoinConditions(Set.of(ChildSideJoinCondition.of("foo", List.of("baz"), "bar")))
        .build();

    ChildSideJoin<Resource, IRI> childSideJoin2 = ChildSideJoin.<Resource, IRI>builder()
        .subjects(subjects2)
        .predicates(predicates)
        .graphs(graphs2)
        .childSideJoinConditions(Set.of(ChildSideJoinCondition.of("foo", List.of("baz"), "bar")))
        .build();

    Set<ChildSideJoin<Resource, IRI>> childSideJoins = new HashSet<>();
    childSideJoins.add(childSideJoin1);
    childSideJoins.add(childSideJoin2);

    when(childSideJoinStoreProvider.create(any())).thenReturn(childSideJoins);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    IRI parentSubject1 = VALUE_FACTORY.createIRI("http://foo.bar/parentSubject1");
    IRI parentSubject2 = VALUE_FACTORY.createIRI("http://foo.bar/parentSubject2");
    Set<Resource> parentSubjects = Set.of(parentSubject1, parentSubject2);

    ParentSideJoinKey parentSideJoinKey = ParentSideJoinKey.of("bar", "baz");

    ConcurrentMap<ParentSideJoinKey, Set<Resource>> parentSideJoinConditions =
        new ConcurrentHashMap<>(Map.of(parentSideJoinKey, parentSubjects));

    when(parentRdfTriplesMapper.getParentSideJoinConditions()).thenReturn(parentSideJoinConditions);

    lenient().when(parentRdfTriplesMapper.notifyCompletion(any(), any()))
        .thenReturn(Mono.when());

    Flux<Statement> joinlessFlux = generateStatementsFor("main", 2);
    Flux<Statement> parentJoinlessFlux = generateStatementsFor("parent", 2);

    Flux<Statement> joinedStatementFlux =
        rdfRefObjectMapper.resolveJoins(joinlessFlux, parentRdfTriplesMapper, parentJoinlessFlux);

    // With deferred expectation
    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject1, predicate1, parentSubject1, graph1),
            VALUE_FACTORY.createStatement(subject1, predicate1, parentSubject2, graph1),
            VALUE_FACTORY.createStatement(subject2, predicate1, parentSubject1, graph2),
            VALUE_FACTORY.createStatement(subject2, predicate1, parentSubject2, graph2))
        .contains(statement);
    StepVerifier deferred = StepVerifier.create(joinedStatementFlux)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .expectComplete()
        .verifyLater();

    // When
    StepVerifier.create(joinlessFlux)
        .expectComplete();

    StepVerifier.create(parentJoinlessFlux)
        .expectComplete();

    // Then
    deferred.verify(Duration.ofSeconds(1));
  }

  private static Flux<Statement> generateStatementsFor(String id, int amount) {
    List<Statement> statements = new ArrayList<>();
    for (int i = 0; i < amount; i++) {
      statements.add(generateStatementFor(id, i));
    }

    return Flux.fromIterable(statements);
  }

  private static Statement generateStatementFor(String id, int number) {
    return VALUE_FACTORY.createStatement(VALUE_FACTORY.createBNode(String.format("sub-%s-%s", id, number)), RDF.TYPE,
        VALUE_FACTORY.createBNode(String.format("obj-%s-%s", id, number)));
  }
}
