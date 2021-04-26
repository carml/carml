package com.taxonic.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TriplesMapper;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinKey;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RdfLogicalSourcePipelineTest {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  @Mock
  private RdfLogicalSourcePipeline<String> rdfLogicalSourcePipeline;

  @Mock
  private LogicalSource logicalSource;

  @Mock
  private LogicalSourceResolver<String> logicalSourceResolver;

  @Mock
  private LogicalSourceResolver.ExpressionEvaluationFactory<String> expressionEvaluatorFactory;

  @Mock
  private RdfMappingContext rdfMappingContext;

  @Mock
  private TriplesMap triplesMapA;

  @Mock
  private TriplesMap triplesMapB;

  @Mock
  private SubjectMap subjectMapA;

  @Mock
  private RdfTriplesMapper<String> rdfTriplesMapperA;

  @Mock
  private RdfTriplesMapper<String> rdfTriplesMapperB;

  @Mock
  private RdfRefObjectMapper rdfRefObjectMapperA1;

  @Mock
  private RdfRefObjectMapper rdfRefObjectMapperA2;

  @Mock
  private RdfRefObjectMapper rdfRefObjectMapperB1;

  @Mock
  private ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider;

  @Mock
  private RdfTermGeneratorFactory rdfTermGeneratorFactory;

  @Mock
  private TermGenerator<Resource> subjectGenerator;

  @Test
  void givenAllParams_whenOfCalled_thenConstructRdfLogicalSourcePipeline() {
    // Given
    List<TriplesMap> triplesMaps = List.of(triplesMapA);

    Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers =
        Map.of(triplesMapA, Set.of(rdfRefObjectMapperA1, rdfRefObjectMapperA2));

    Map<RdfRefObjectMapper, TriplesMap> incomingRoMapperToParentTm = Map.of(rdfRefObjectMapperB1, triplesMapA);

    when(logicalSourceResolver.getExpressionEvaluationFactory()).thenReturn(expressionEvaluatorFactory);

    when(triplesMapA.getSubjectMap()).thenReturn(subjectMapA);
    when(rdfMappingContext.getTermGeneratorFactory()).thenReturn(rdfTermGeneratorFactory);
    when(rdfMappingContext.getValueFactorySupplier()).thenReturn(SimpleValueFactory::getInstance);
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMapA)).thenReturn(subjectGenerator);
    ConcurrentMap<ParentSideJoinKey, Set<Resource>> parentSideJoinConditions = new ConcurrentHashMap<>();
    when(parentSideJoinConditionStoreProvider.create(any())).thenReturn(parentSideJoinConditions);

    // When
    RdfLogicalSourcePipeline<String> rdfLogicalSourcePipeline =
        RdfLogicalSourcePipeline.of(logicalSource, triplesMaps, tmToRoMappers, incomingRoMapperToParentTm,
            logicalSourceResolver, rdfMappingContext, parentSideJoinConditionStoreProvider);

    // Then
    assertThat(rdfLogicalSourcePipeline.getLogicalSource(), is(logicalSource));
    assertThat(rdfLogicalSourcePipeline.getLogicalSourceResolver(), is(logicalSourceResolver));
    assertThat(rdfLogicalSourcePipeline.getRdfMappingContext(), is(rdfMappingContext));
    assertThat(rdfLogicalSourcePipeline.getTriplesMappers()
        .size(), is(1));

    RdfTriplesMapper<String> rdfTriplesMapper = Iterables.getOnlyElement(rdfLogicalSourcePipeline.getTriplesMappers());
    assertThat(rdfTriplesMapper.getTriplesMap(), is(triplesMapA));
    assertThat(rdfTriplesMapper.getIncomingRefObjectMappers()
        .size(), is(1));

    assertThat(rdfTriplesMapper.getIncomingRefObjectMappers(), hasItem(rdfRefObjectMapperB1));
  }

  @Test
  void givenJoinlessTriplesMappers_whenMapAndSubscribe_thenProduceExpectedStatements() {
    // Given
    doReturn(generateStatementsFor("A", 2)).when(rdfTriplesMapperA)
        .map(any(String.class));
    doReturn(generateStatementsFor("B", 4)).when(rdfTriplesMapperB)
        .map(any(String.class));

    Set<RdfTriplesMapper<String>> triplesMappers = Set.of(rdfTriplesMapperA, rdfTriplesMapperB);

    when(logicalSourceResolver.getSourceFlux()).thenReturn((foo, bar) -> Flux.just("one", "two"));

    rdfLogicalSourcePipeline =
        RdfLogicalSourcePipeline.of(logicalSource, logicalSourceResolver, rdfMappingContext, triplesMappers);

    Map<TriplesMapper<String, Statement>, Flux<Statement>> pipelineResult = rdfLogicalSourcePipeline.run();

    StepVerifier deferredA = StepVerifier.create(pipelineResult.get(rdfTriplesMapperA))
        .expectNextCount(4)
        .expectComplete()
        .verifyLater();

    StepVerifier deferredB = StepVerifier.create(pipelineResult.get(rdfTriplesMapperB))
        .expectNextCount(8)
        .expectComplete()
        .verifyLater();

    // When
    Flux.merge(pipelineResult.values())
        .subscribe();

    // Then
    deferredA.verify();
    deferredB.verify();
  }

  // @Test
  // void givenTriplesMapperWithJoins_whenMapAndSubscribe_thenProduceExpectedStatements() {
  // // Given
  // doReturn(generateStatementsFor("A", 2)).when(rdfTriplesMapperA)
  // .map(any(String.class));
  // doReturn(generateStatementsFor("B", 4)).when(rdfTriplesMapperB)
  // .map(any(String.class));
  //
  // when(rdfTriplesMapperA.getTriplesMap()).thenReturn(triplesMapA);
  // when(rdfTriplesMapperB.getTriplesMap()).thenReturn(triplesMapB);
  //
  // Set<RdfTriplesMapper<String>> triplesMappers = Set.of(rdfTriplesMapperA, rdfTriplesMapperB);
  //
  // when(rdfTriplesMapperA.streamConnectedRefObjectMappers())
  // .thenReturn(Stream.of(rdfRefObjectMapperA1, rdfRefObjectMapperA2));
  //
  // when(rdfRefObjectMapperA1.signalCompletion(any())).thenReturn(Mono.when());
  // when(rdfRefObjectMapperA2.signalCompletion(any())).thenReturn(Mono.when());
  //
  // when(rdfTriplesMapperA.streamRefObjectMappers()).thenReturn(Stream.of(rdfRefObjectMapperA1));
  //
  // ConnectableFlux<Statement> connectableJoinResult = generateStatementsFor("A1", 3).publish();
  // doReturn(connectableJoinResult).when(rdfRefObjectMapperA1)
  // .resolveJoins();
  //
  // when(logicalSourceResolver.getSourceFlux()).thenReturn((foo, bar) -> Flux.just("one", "two"));
  //
  // rdfLogicalSourcePipeline =
  // RdfLogicalSourcePipeline.of(logicalSource, logicalSourceResolver, rdfMappingContext,
  // triplesMappers);
  //
  // Map<TriplesMap, Flux<Statement>> pipelineResult = rdfLogicalSourcePipeline.run();
  //
  // StepVerifier deferredA = StepVerifier.create(pipelineResult.get(triplesMapA))
  // .expectNextCount(7) // 2 x 2 (joinless) + 3 (joins)
  // .expectComplete()
  // .verifyLater();
  //
  // StepVerifier deferredB = StepVerifier.create(pipelineResult.get(triplesMapB))
  // .expectNextCount(8)
  // .expectComplete()
  // .verifyLater();
  //
  // // When
  // Flux.merge(pipelineResult.values())
  // .subscribe();
  //
  // connectableJoinResult.connect();
  //
  // // Then
  // deferredA.verify();
  // deferredB.verify();
  //
  // verify(rdfRefObjectMapperA1, times(1)).signalCompletion(rdfTriplesMapperA);
  // verify(rdfRefObjectMapperA2, times(1)).signalCompletion(rdfTriplesMapperA);
  // }

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
