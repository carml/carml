package io.carml.engine.rdf;

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
import io.carml.engine.ExpressionEvaluation;
import io.carml.engine.join.ChildSideJoin;
import io.carml.engine.join.ChildSideJoinCondition;
import io.carml.engine.join.ChildSideJoinStore;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.engine.join.ParentSideJoinKey;
import io.carml.model.Join;
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
  private ChildSideJoinStore<Resource, IRI> childSideJoinStore;

  @Mock
  private RdfTriplesMapper<?> parentRdfTriplesMapper;

  @Mock
  private ParentSideJoinConditionStore<Resource> parentSideJoinConditionStore;

  @Mock
  Mono<Void> completion;

  @Captor
  private ArgumentCaptor<Set<ChildSideJoin<Resource, IRI>>> childSideJoinCaptor;

  @BeforeEach
  void setup() {
    when(childSideJoinStoreProvider.createChildSideJoinStore(any())).thenReturn(childSideJoinStore);
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

    when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1));
    when(join1.getChild()).thenReturn("foo");
    when(join1.getParent()).thenReturn("bar");

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

    Set<Resource> subjects = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/subject1"));
    Set<Resource> graphs = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/graph1"));

    Map<Set<Resource>, Set<Resource>> subjectsAndAllGraphs = Map.of(subjects, graphs);

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    Set<IRI> predicates = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/predicate1"));

    // When
    Mono<Statement> refObjectMapperPromise = Mono.empty();
    rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

    // Then
    StepVerifier.create(refObjectMapperPromise)
        .expectComplete()
        .verify();

    verify(childSideJoinStore, times(1)).addAll(childSideJoinCaptor.capture());

    Set<ChildSideJoin<Resource, IRI>> childSideJoins = childSideJoinCaptor.getValue();

    ChildSideJoin<Resource, IRI> childSideJoin = Iterables.getOnlyElement(childSideJoins);

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

    when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1));
    when(join1.getChild()).thenReturn("foo");
    when(join1.getParent()).thenReturn("bar");

    Resource subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    Resource subject2 = VALUE_FACTORY.createIRI("http://foo.bar/subject2");
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    Resource graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph1");
    Resource graph2 = VALUE_FACTORY.createIRI("http://foo.bar/graph2");
    Set<Resource> subjects = Set.of(subject1);
    Set<Resource> subjects2 = Set.of(subject2);
    Set<Resource> graphs = Set.of(graph1);
    Set<Resource> graphs2 = Set.of(graph2);

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

    Map<Set<Resource>, Set<Resource>> subjectsAndAllGraphs = Map.of(subjects, graphs, subjects2, graphs2);

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    Set<IRI> predicates = Set.of(predicate1);

    // When
    Mono<Statement> refObjectMapperPromise = Mono.empty();
    rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

    // Then
    StepVerifier.create(refObjectMapperPromise)
        .expectComplete()
        .verify();

    verify(childSideJoinStore, times(1)).addAll(childSideJoinCaptor.capture());

    Set<ChildSideJoin<Resource, IRI>> childSideJoins = childSideJoinCaptor.getValue();

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
    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    when(refObjectMap.getJoinConditions()).thenReturn(Set.of(join1, join2));
    when(join1.getChild()).thenReturn("foo");
    when(join1.getParent()).thenReturn("bar");

    when(join2.getChild()).thenReturn("Alice");
    when(join2.getParent()).thenReturn("Bob");

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")))
        .thenReturn(Optional.of(List.of("Carol")));

    Set<Resource> subjects = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/subject1"));
    Set<Resource> graphs = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/graph1"));

    Map<Set<Resource>, Set<Resource>> subjectsAndAllGraphs = Map.of(subjects, graphs);

    Set<IRI> predicates = Set.of(VALUE_FACTORY.createIRI("http://foo.bar/predicate1"));

    RdfRefObjectMapper rdfRefObjectMapper =
        RdfRefObjectMapper.of(refObjectMap, triplesMap, rdfMappingContext, childSideJoinStoreProvider);

    // When
    Mono<Statement> refObjectMapperPromise = Mono.empty();
    rdfRefObjectMapper.map(subjectsAndAllGraphs, predicates, expressionEvaluation);

    // Then
    StepVerifier.create(refObjectMapperPromise)
        .verifyComplete();

    verify(childSideJoinStore, times(1)).addAll(childSideJoinCaptor.capture());

    Set<ChildSideJoin<Resource, IRI>> childSideJoins = childSideJoinCaptor.getValue();

    ChildSideJoin<Resource, IRI> childSideJoin = Iterables.getOnlyElement(childSideJoins);

    assertThat(childSideJoin.getSubjects(), is(subjects));
    assertThat(childSideJoin.getPredicates(), is(predicates));
    assertThat(childSideJoin.getGraphs(), is(graphs));

    Set<ChildSideJoinCondition> childSideJoinConditions = childSideJoin.getChildSideJoinConditions();

    assertThat(childSideJoinConditions, hasSize(2));
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
        .subjects(new HashSet<>(subjects))
        .predicates(new HashSet<>(predicates))
        .graphs(new HashSet<>(graphs))
        .childSideJoinConditions(
            new HashSet<>(Set.of(ChildSideJoinCondition.of("foo", new ArrayList<>(List.of("baz")), "bar"))))
        .build();

    when(childSideJoinStore.clearingFlux()).thenReturn(Flux.just(childSideJoin1));

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

    when(parentSideJoinConditionStore.containsKey(parentSideJoinKey)).thenReturn(true);
    when(parentSideJoinConditionStore.get(parentSideJoinKey)).thenReturn(parentSubjects);

    when(parentRdfTriplesMapper.getParentSideJoinConditions()).thenReturn(parentSideJoinConditionStore);

    // When
    Flux<Statement> joinedStatementFlux = rdfRefObjectMapper.resolveJoins(parentRdfTriplesMapper);

    // Then
    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject1, predicate1, parentSubject1, graph1),
            VALUE_FACTORY.createStatement(subject1, predicate1, parentSubject2, graph1))
        .contains(statement);

    StepVerifier.create(joinedStatementFlux)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .verifyComplete();
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
        .subjects(new HashSet<>(subjects))
        .predicates(new HashSet<>(predicates))
        .graphs(new HashSet<>(graphs))
        .childSideJoinConditions(
            new HashSet<>(Set.of(ChildSideJoinCondition.of("foo", new ArrayList<>(List.of("baz")), "bar"))))
        .build();

    ChildSideJoin<Resource, IRI> childSideJoin2 = ChildSideJoin.<Resource, IRI>builder()
        .subjects(new HashSet<>(subjects2))
        .predicates(new HashSet<>(predicates))
        .graphs(new HashSet<>(graphs2))
        .childSideJoinConditions(
            new HashSet<>(Set.of(ChildSideJoinCondition.of("foo", new ArrayList<>(List.of("baz")), "bar"))))
        .build();

    when(childSideJoinStore.clearingFlux()).thenReturn(Flux.just(childSideJoin1, childSideJoin2));

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

    when(parentSideJoinConditionStore.containsKey(parentSideJoinKey)).thenReturn(true);
    when(parentSideJoinConditionStore.get(parentSideJoinKey)).thenReturn(parentSubjects);

    when(parentRdfTriplesMapper.getParentSideJoinConditions()).thenReturn(parentSideJoinConditionStore);

    // When
    Flux<Statement> joinedStatementFlux = rdfRefObjectMapper.resolveJoins(parentRdfTriplesMapper);

    // Then
    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject1, predicate1, parentSubject1, graph1),
            VALUE_FACTORY.createStatement(subject1, predicate1, parentSubject2, graph1),
            VALUE_FACTORY.createStatement(subject2, predicate1, parentSubject1, graph2),
            VALUE_FACTORY.createStatement(subject2, predicate1, parentSubject2, graph2))
        .contains(statement);

    StepVerifier.create(joinedStatementFlux)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .verifyComplete();
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
