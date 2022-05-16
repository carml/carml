package com.taxonic.carml.engine.rdf;

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

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TriplesMapperException;
import com.taxonic.carml.engine.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.ObjectMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.vocab.Rdf;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfPredicateObjectMapperTest {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  @Mock
  private PredicateObjectMap pom;

  @Mock
  private TriplesMap triplesMap;

  @Mock
  private LogicalSource logicalSource;

  @Mock
  private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider;

  @Mock
  private RdfTermGeneratorFactory rdfTermGeneratorFactory;

  @Mock
  private Set<Resource> subjects;

  @Mock
  private Set<Resource> subjectGraphs;

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
    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    // When
    RdfPredicateObjectMapper rdfPredicateObjectMapper =
        RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingContext);

    // Then
    assertThat(rdfPredicateObjectMapper, is(not(nullValue())));
    assertThat(rdfPredicateObjectMapper.getRdfRefObjectMappers(), is(empty()));
  }

  @Test
  void givenSingleJoinlessRefObjectMapWithDifferentLogicalSource_whenOfCalled_thenThrowException() {
    // Given
    when(triplesMap.getLogicalSource()).thenReturn(logicalSource);
    when(triplesMap.asRdf()).thenReturn(new ModelBuilder().build());
    when(logicalSource.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("logicalSource"));

    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    subjects = Set.of(subject1);

    when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
    when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);

    when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));
    when(refObjectMap1.getJoinConditions()).thenReturn(Set.of());
    when(refObjectMap1.getParentTriplesMap()).thenReturn(triplesMap2);
    when(refObjectMap1.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("refObjectMap1"));

    when(triplesMap2.getLogicalSource()).thenReturn(logicalSource2);
    when(triplesMap2.asRdf()).thenReturn(new ModelBuilder().build());
    when(logicalSource2.getAsResource()).thenReturn(VALUE_FACTORY.createBNode("logicalSource2"));

    IRI subjectGraph1 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph1");
    subjectGraphs = Set.of(subjectGraph1);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    // When
    Throwable exception = assertThrows(TriplesMapperException.class,
        () -> RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingContext));

    // Then
    assertThat(exception.getMessage(), startsWith("Logical sources are not equal."));
  }

  @Test
  void givenPredicateGeneratorReturningEmpty_whenMap_thenReturnNothing() {
    // Given
    when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
    when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
    when(predicateGenerator1.apply(any())).thenReturn(List.of());

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfPredicateObjectMapper rdfPredicateObjectMapper =
        RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingContext);

    Map<Set<Resource>, Set<Resource>> subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

    // When
    Flux<Statement> pomStatements = rdfPredicateObjectMapper.map(any(), subjectsAndSubjectGraphs);

    // Then
    StepVerifier.create(pomStatements)
        .verifyComplete();
  }

  @Test
  void givenSingleValuedSubPredObjGenerators_whenMap_thenGenerateSingleStatement() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    subjects = Set.of(subject1);

    when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
    when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    when(predicateGenerator1.apply(any())).thenReturn(List.of(predicate1));

    when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
    when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
    Value object1 = VALUE_FACTORY.createLiteral("object1");
    when(objectGenerator1.apply(any())).thenReturn(List.of(object1));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfPredicateObjectMapper rdfPredicateObjectMapper =
        RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingContext);

    Map<Set<Resource>, Set<Resource>> subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

    // When
    Flux<Statement> pomStatements = rdfPredicateObjectMapper.map(any(), subjectsAndSubjectGraphs);

    // Then
    StepVerifier.create(pomStatements)
        .expectNext(VALUE_FACTORY.createStatement(subject1, predicate1, object1))
        .verifyComplete();
  }

  @Test
  void givenSubjectGraphsAndGraphGenerators_whenMap_generatesStatementsForAllGraphs() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    subjects = Set.of(subject1);

    when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
    when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    when(predicateGenerator1.apply(any())).thenReturn(List.of(predicate1));

    when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
    when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
    Value object1 = VALUE_FACTORY.createLiteral("object1");
    when(objectGenerator1.apply(any())).thenReturn(List.of(object1));

    IRI subjectGraph1 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph1");
    subjectGraphs = Set.of(subjectGraph1);

    when(pom.getGraphMaps()).thenReturn(Set.of(graphMap1));
    when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
    IRI graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph");
    when(graphGenerator1.apply(any())).thenReturn(List.of(graph1));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfPredicateObjectMapper rdfPredicateObjectMapper =
        RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingContext);

    Map<Set<Resource>, Set<Resource>> subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

    // When
    Flux<Statement> pomStatements = rdfPredicateObjectMapper.map(any(), subjectsAndSubjectGraphs);

    // Then
    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject1, predicate1, object1, subjectGraph1),
            VALUE_FACTORY.createStatement(subject1, predicate1, object1, graph1))
        .contains(statement);
    StepVerifier.create(pomStatements)
        .expectNextMatches(expectedStatement)
        .expectNextMatches(expectedStatement)
        .verifyComplete();
  }

  @Test
  void givenSubjectGraphsWithDefaultGraphAndGraphGeneratorsWithDefaultGraph_whenMap_generatesStatementsForAllGraphs() {
    // Given
    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    subjects = Set.of(subject1);

    when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
    when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    when(predicateGenerator1.apply(any())).thenReturn(List.of(predicate1));

    when(pom.getObjectMaps()).thenReturn(Set.of(objectMap1));
    when(rdfTermGeneratorFactory.getObjectGenerator(objectMap1)).thenReturn(objectGenerator1);
    Value object1 = VALUE_FACTORY.createLiteral("object1");
    when(objectGenerator1.apply(any())).thenReturn(List.of(object1));

    IRI subjectGraph1 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph1");
    subjectGraphs = Set.of(subjectGraph1, Rdf.Rr.defaultGraph);

    when(pom.getGraphMaps()).thenReturn(Set.of(graphMap1));
    when(rdfTermGeneratorFactory.getGraphGenerator(graphMap1)).thenReturn(graphGenerator1);
    IRI graph1 = VALUE_FACTORY.createIRI("http://foo.bar/graph1");
    when(graphGenerator1.apply(any())).thenReturn(List.of(graph1, Rdf.Rr.defaultGraph));

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfPredicateObjectMapper rdfPredicateObjectMapper =
        RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingContext);

    Map<Set<Resource>, Set<Resource>> subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

    // When
    Flux<Statement> pomStatements = rdfPredicateObjectMapper.map(any(), subjectsAndSubjectGraphs);

    // Then
    Predicate<Statement> expectedStatement = statement -> Set
        .of(VALUE_FACTORY.createStatement(subject1, predicate1, object1, subjectGraph1),
            VALUE_FACTORY.createStatement(subject1, predicate1, object1, graph1),
            VALUE_FACTORY.createStatement(subject1, predicate1, object1))
        .contains(statement);
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

    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    subjects = Set.of(subject1);

    when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
    when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    when(predicateGenerator1.apply(any())).thenReturn(List.of(predicate1));

    when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));
    when(refObjectMap1.getJoinConditions()).thenReturn(Set.of());
    when(refObjectMap1.getParentTriplesMap()).thenReturn(triplesMap2);

    when(triplesMap2.getLogicalSource()).thenReturn(logicalSource);
    when(triplesMap2.getSubjectMaps()).thenReturn(Set.of(subjectMap2));

    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap2)).thenReturn(subjectGenerator2);

    IRI subject2 = VALUE_FACTORY.createIRI("http://foo.bar/subject2");
    when(subjectGenerator2.apply(any())).thenReturn(List.of(subject2));

    IRI subjectGraph1 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph1");
    subjectGraphs = Set.of(subjectGraph1);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfPredicateObjectMapper rdfPredicateObjectMapper =
        RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingContext);

    Map<Set<Resource>, Set<Resource>> subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

    // When
    Flux<Statement> pomStatements = rdfPredicateObjectMapper.map(any(), subjectsAndSubjectGraphs);

    // Then
    StepVerifier.create(pomStatements)
        .expectNext(VALUE_FACTORY.createStatement(subject1, predicate1, subject2, subjectGraph1))
        .verifyComplete();
  }

  @Test
  void givenSingleJoinSingleConditionedRefObjectMap_whenMap_thenChildSideJoinConditionCached() {
    // Given
    when(triplesMap.getLogicalSource()).thenReturn(logicalSource);

    IRI subject1 = VALUE_FACTORY.createIRI("http://foo.bar/subject1");
    subjects = Set.of(subject1);

    when(pom.getPredicateMaps()).thenReturn(Set.of(predicateMap1));
    when(pom.getObjectMaps()).thenReturn(Set.of(refObjectMap1));

    when(rdfTermGeneratorFactory.getPredicateGenerator(predicateMap1)).thenReturn(predicateGenerator1);
    IRI predicate1 = VALUE_FACTORY.createIRI("http://foo.bar/predicate1");
    when(predicateGenerator1.apply(any())).thenReturn(List.of(predicate1));

    when(refObjectMap1.getJoinConditions()).thenReturn(Set.of(join1));

    IRI subjectGraph1 = VALUE_FACTORY.createIRI("http://foo.bar/subjectGraph1");
    subjectGraphs = Set.of(subjectGraph1);

    rdfRefObjectMappers = Set.of(rdfRefObjectMapper1);
    when(rdfRefObjectMapper1.getRefObjectMap()).thenReturn(refObjectMap1);

    RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
        .valueFactorySupplier(() -> VALUE_FACTORY)
        .termGeneratorFactory(rdfTermGeneratorFactory)
        .childSideJoinStoreProvider(childSideJoinStoreProvider)
        .build();

    RdfPredicateObjectMapper rdfPredicateObjectMapper =
        RdfPredicateObjectMapper.of(pom, triplesMap, rdfRefObjectMappers, rdfMappingContext);

    ExpressionEvaluation expressionEvaluation = mock(ExpressionEvaluation.class);

    Map<Set<Resource>, Set<Resource>> subjectsAndSubjectGraphs = Map.of(subjects, subjectGraphs);

    // When
    Flux<Statement> pomStatements = rdfPredicateObjectMapper.map(expressionEvaluation, subjectsAndSubjectGraphs);

    // Then
    verify(rdfRefObjectMapper1, times(1)).map(subjectsAndSubjectGraphs, Set.of(predicate1), expressionEvaluation);

    StepVerifier.create(pomStatements)
        .verifyComplete();
  }

}
