package com.taxonic.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TriplesMapperException;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinKey;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.vocab.Rdf;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfTriplesMapperTest {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  @Mock
  private TriplesMap triplesMap;

  @Mock
  private TermGenerator<Resource> subjectGenerator;

  @Mock
  private SubjectMap subjectMap;

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
  private RefObjectMap refObjectMap1;

  @Mock
  private RefObjectMap refObjectMap2;

  @Mock
  private RdfRefObjectMapper rdfRefObjectMapper1;

  @Mock
  private RdfRefObjectMapper rdfRefObjectMapper2;

  private Set<RdfRefObjectMapper> incomingRefObjectMappers;

  @Mock
  private RdfRefObjectMapper incomingRdfRefObjectMapper1;

  @Mock
  private LogicalSourceResolver.ExpressionEvaluationFactory<String> expressionEvaluatorFactory;

  @Mock
  private ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider;

  private ConcurrentMap<ParentSideJoinKey, Set<Resource>> parentSideJoinConditions;

  @Mock
  private RdfTermGeneratorFactory rdfTermGeneratorFactory;

  @Mock
  private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider;

  @Mock
  private ExpressionEvaluation expressionEvaluation;

  @Mock
  private Join join1;

  @Mock
  private Join join2;

  @Mock
  private Join join3;

  @BeforeEach
  void setup() {
    refObjectMappers = new HashSet<>();
    incomingRefObjectMappers = new HashSet<>();
    when(triplesMap.getSubjectMap()).thenReturn(subjectMap);
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
    when(triplesMap.getPredicateObjectMaps()).thenReturn(Set.of(pom));
    parentSideJoinConditions = new ConcurrentHashMap<>();
    when(parentSideJoinConditionStoreProvider.create(any())).thenReturn(parentSideJoinConditions);
  }

  @Test
  void givenAllParams_whenOfCalled_thenConstructRdfPredicateObjectMapper() {
    // Given
    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    // When
    RdfTriplesMapper<?> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers, incomingRefObjectMappers,
        expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    // Then
    assertThat(rdfTriplesMapper, is(not(nullValue())));
    assertThat(rdfTriplesMapper.getRefObjectMappers(), is(empty()));
    assertThat(rdfTriplesMapper.getConnectedRefObjectMappers(), is(empty()));
  }

  @Test
  void givenOnlySubjectMapWithClass_whenMap_thenReturnTypeStatement() {
    // Given
    IRI subject = VALUE_FACTORY.createIRI("http://foo.bar/subject");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
    IRI class1 = VALUE_FACTORY.createIRI("http://foo.bar/class1");
    when(subjectMap.getClasses()).thenReturn(Set.of(class1));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers,
        incomingRefObjectMappers, expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    // When
    Flux<Statement> statements = rdfTriplesMapper.map("foo");

    // Then
    StepVerifier.create(statements)
        .expectNext(VALUE_FACTORY.createStatement(subject, RDF.TYPE, class1))
        .verifyComplete();
  }

  @Test
  void givenSubjectMapThatReturnsNothing_whenMap_thenReturnEmptyFlux() {
    // Given
    when(subjectGenerator.apply(any())).thenReturn(List.of());

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers,
        incomingRefObjectMappers, expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    // When
    Flux<Statement> statements = rdfTriplesMapper.map("foo");

    // Then
    StepVerifier.create(statements)
        .expectNextCount(0)
        .verifyComplete();
  }

  @Test
  void givenSubjectMapAndPom_whenMap_thenReturnStatements() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject1));
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject1));
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject1));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

    when(subjectMap.getGraphMaps()).thenReturn(Set.of(graphMap1));
    when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
    IRI subjectGraph1 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph1");
    when(graphGenerator1.apply(any())).thenReturn(List.of(subjectGraph1, Rdf.Rr.defaultGraph));

    when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
    when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    when(predicateGenerator1.apply(any())).thenReturn(List.of(predicate1));

    when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
    when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
    Value object1 = VALUE_FACTORY.createLiteral("object1");
    when(objectGenerator1.apply(any())).thenReturn(List.of(object1));

    when(pom.getGraphMaps()).thenReturn(Set.of(graphMap2));
    when(rdfTermGeneratorFactory.getGraphGenerator(graphMap2)).thenReturn(graphGenerator2);
    IRI graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph1");
    when(graphGenerator2.apply(any())).thenReturn(List.of(graph1, Rdf.Rr.defaultGraph));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers,
        incomingRefObjectMappers, expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    // When
    Flux<Statement> statements = rdfTriplesMapper.map("foo");

    // Then
    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject1, predicate1, object1, subjectGraph1),
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
  void givenSubjectMapAndIncomingRefObjectMappers_whenMap_thenCacheParentSideJoinConditions() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject1));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

    when(rdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap1);
    when(refObjectMap1.getJoinConditions()).thenReturn(Set.of(join1));
    when(join1.getParentReference()).thenReturn("bar1");

    when(rdfRefObjectMapper2.getRefObjectMap()).thenReturn(refObjectMap2);

    when(refObjectMap2.getJoinConditions()).thenReturn(Set.of(join2, join3));
    when(join2.getParentReference()).thenReturn("bar2");
    when(join3.getParentReference()).thenReturn("bar3");

    incomingRefObjectMappers = Set.of(rdfRefObjectMapper1, rdfRefObjectMapper2);

    when(expressionEvaluatorFactory.apply(any())).thenReturn(expressionEvaluation);
    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers,
        incomingRefObjectMappers, expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    // When
    Flux<Statement> statements = rdfTriplesMapper.map("foo");

    // Then
    StepVerifier.create(statements)
        .verifyComplete();

    assertThat(parentSideJoinConditions, hasEntry(ParentSideJoinKey.of("bar1", "baz"), Set.of(subject1)));
    assertThat(parentSideJoinConditions, hasEntry(ParentSideJoinKey.of("bar2", "baz"), Set.of(subject1)));
    assertThat(parentSideJoinConditions, hasEntry(ParentSideJoinKey.of("bar3", "baz"), Set.of(subject1)));
  }

  @Test
  void givenIncomingRefObjectMapperWithSameParentKeyInMultipleMaps_whenMap_thenAddToExistingParentSideJoinConditions() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    IRI subject2 = VALUE_FACTORY.createIRI("http://foo.bar/subject2");
    IRI subject3 = VALUE_FACTORY.createIRI("http://foo.bar/subject3");

    when(subjectGenerator.apply(any())).thenReturn(List.of(subject1))
        .thenReturn(List.of(subject2))
        .thenReturn(List.of(subject3));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

    when(rdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap1);
    when(refObjectMap1.getJoinConditions()).thenReturn(Set.of(join1));
    when(join1.getParentReference()).thenReturn("bar1");

    incomingRefObjectMappers = Set.of(rdfRefObjectMapper1);

    when(expressionEvaluatorFactory.apply(any())).thenReturn(expressionEvaluation);
    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers,
        incomingRefObjectMappers, expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    // When
    rdfTriplesMapper.map("foo");

    // Then
    assertThat(parentSideJoinConditions, hasEntry(ParentSideJoinKey.of("bar1", "baz"), Set.of(subject1)));

    // When
    rdfTriplesMapper.map("foo");

    // Then
    assertThat(parentSideJoinConditions, hasEntry(ParentSideJoinKey.of("bar1", "baz"), Set.of(subject1, subject2)));

    // When
    rdfTriplesMapper.map("foo");

    // Then
    assertThat(parentSideJoinConditions,
        hasEntry(ParentSideJoinKey.of("bar1", "baz"), Set.of(subject1, subject2, subject3)));
  }

  @Test
  void givenTriplesMapperWithIncomingRefObjectMappers_whenAllNotifyCompletion_thenCleanUpParentSideJoins() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    when(subjectGenerator.apply(any())).thenReturn(List.of(subject1));
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);

    when(rdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap1);
    when(refObjectMap1.getJoinConditions()).thenReturn(Set.of(join1));
    when(join1.getParentReference()).thenReturn("bar1");

    when(rdfRefObjectMapper2.getRefObjectMap()).thenReturn(refObjectMap2);

    when(refObjectMap2.getJoinConditions()).thenReturn(Set.of(join2, join3));
    when(join2.getParentReference()).thenReturn("bar2");
    when(join3.getParentReference()).thenReturn("bar3");

    incomingRefObjectMappers = Set.of(rdfRefObjectMapper1, rdfRefObjectMapper2);

    when(expressionEvaluatorFactory.apply(any())).thenReturn(expressionEvaluation);
    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("baz")));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers,
        incomingRefObjectMappers, expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    // When
    Flux<Statement> statements = rdfTriplesMapper.map("foo");
    Mono<Void> donePromise1 = rdfTriplesMapper.notifyCompletion(rdfRefObjectMapper1, SignalType.ON_COMPLETE);
    Mono<Void> donePromise2 = rdfTriplesMapper.notifyCompletion(rdfRefObjectMapper2, SignalType.ON_COMPLETE);

    // Then
    StepVerifier.create(statements)
        .verifyComplete();

    StepVerifier.create(donePromise1)
        .verifyComplete();

    assertThat(parentSideJoinConditions, hasEntry(ParentSideJoinKey.of("bar1", "baz"), Set.of(subject1)));
    assertThat(parentSideJoinConditions, hasEntry(ParentSideJoinKey.of("bar2", "baz"), Set.of(subject1)));
    assertThat(parentSideJoinConditions, hasEntry(ParentSideJoinKey.of("bar3", "baz"), Set.of(subject1)));

    StepVerifier.create(donePromise2)
        .verifyComplete();

    assertThat(parentSideJoinConditions.size(), is(0));
  }

  @Test
  void givenTriplesMapperWithIncomingRefObjectMapper_whenUnknownNotifiesCompletion_thenThrowException() {
    // Given
    incomingRefObjectMappers = Set.of(rdfRefObjectMapper1);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers,
        incomingRefObjectMappers, expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    when(incomingRdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap2);
    when(refObjectMap2.asRdf()).thenReturn(new ModelBuilder().build());
    when(refObjectMap2.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("refObjectMap2"));
    when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
    when(triplesMap.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("triplesMap"));

    // When
    Throwable exception = assertThrows(TriplesMapperException.class,
        () -> rdfTriplesMapper.notifyCompletion(incomingRdfRefObjectMapper1, SignalType.ON_COMPLETE));

    // Then
    assertThat(exception.getMessage(), startsWith("Provided refObjectMap(per) for"));
  }

  @Test
  void givenIncomingRefObjectMapper_whenNotifiesCompletionWithUnsupportedSignal_thenThrowException() {
    // Given
    incomingRefObjectMappers = Set.of(rdfRefObjectMapper1);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfTriplesMapper<String> rdfTriplesMapper = RdfTriplesMapper.of(triplesMap, refObjectMappers,
        incomingRefObjectMappers, expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider);

    when(rdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap1);
    when(refObjectMap1.asRdf()).thenReturn(new ModelBuilder().build());
    when(refObjectMap1.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("refObjectMap1"));
    when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
    when(triplesMap.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("triplesMap"));

    // When
    Throwable exception = assertThrows(TriplesMapperException.class,
        () -> rdfTriplesMapper.notifyCompletion(rdfRefObjectMapper1, SignalType.ON_SUBSCRIBE));

    // Then
    assertThat(exception.getMessage(), startsWith("Provided refObjectMapper for"));
  }

}
