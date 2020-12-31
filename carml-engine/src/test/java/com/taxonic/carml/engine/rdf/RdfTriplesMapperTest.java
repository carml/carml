package com.taxonic.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinKey;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RdfTriplesMapperTest {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  @Mock
  private TriplesMap triplesMap;

  @Mock
  private SubjectMap subjectMap;

  @Mock
  private TermGenerator<Resource> subjectGenerator;

  private Set<RdfRefObjectMapper> refObjectMappers;

  @Mock
  private RdfRefObjectMapper rdfRefObjectMapper1;

  private Set<RdfRefObjectMapper> incomingRefObjectMappers;

  @Mock
  private RdfRefObjectMapper incomingRdfRefObjectMapper1;

  @Mock
  private LogicalSourceResolver.ExpressionEvaluationFactory<?> expressionEvaluatorFactory;

  @Mock
  private ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider;

  private ConcurrentMap<ParentSideJoinKey, Set<Resource>> parentSideJoinConditions;

  @Mock
  private RdfTermGeneratorFactory rdfTermGeneratorFactory;

  @Mock
  private ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider;

  @BeforeEach
  void setup() {
    refObjectMappers = new HashSet<>();
    incomingRefObjectMappers = new HashSet<>();
    when(triplesMap.getSubjectMap()).thenReturn(subjectMap);
    when(rdfTermGeneratorFactory.getSubjectGenerator(subjectMap)).thenReturn(subjectGenerator);
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
    assertThat(rdfTriplesMapper.streamRefObjectMappers()
        .collect(Collectors.toList()), is(empty()));
    assertThat(rdfTriplesMapper.streamConnectedRefObjectMappers()
        .collect(Collectors.toList()), is(empty()));
  }

  // @Test
  // void given_whenMap_thenReturn() {
  //
  // }

  // private Set<TriplesMap> mapping;
  //
  // private CsvResolver csvResolver;
  //
  // Set<RdfTriplesMapper<Record>> triplesMappers;

  // @BeforeEach
  // public void setup() {
  // mapping = RmlMappingLoader.build()
  // .load(RDFFormat.TURTLE, RdfTriplesMapperTest.class.getResourceAsStream("cars.rml.ttl"));
  //
  // csvResolver = CsvResolver.getInstance();
  //
  // LogicalSourceResolver.ExpressionEvaluationFactory<Record> expressionEvaluatorFactory =
  // csvResolver.getExpressionEvaluationFactory();
  //
  // RdfMappingContext rdfMappingContext = RdfMappingContext.builder()
  // .valueFactorySupplier(SimpleValueFactory::getInstance)
  // .childSideJoinStoreProvider(MapDbChildSideJoinStoreProvider.getInstance())
  // .termGeneratorFactory(RdfTermGeneratorFactory.of(SimpleValueFactory.getInstance(),
  // RdfMapperOptions.builder()
  // .normalizationForm(Normalizer.Form.NFC)
  // .build(), TemplateParser.build()))
  // .build();
  //
  // triplesMappers = mapping.stream()
  // .map(triplesMap -> RdfTriplesMapper.of(triplesMap, Set.of(), Set.of(),
  // expressionEvaluatorFactory,
  // rdfMappingContext, MapDbParentSideJoinConditionStoreProvider.getInstance()))
  // .collect(ImmutableSet.toImmutableSet());
  // }
  //
  // @Test
  // public void map_should_work() {
  //
  // Scheduler schedulerA = Schedulers.newParallel("scheduler-a", 4);
  // Scheduler schedulerB = Schedulers.newParallel("scheduler-b", 4);
  //
  // Flux<Record> recordFlux = csvResolver.getSourceFlux()
  // .apply(RdfTriplesMapperTest.class.getResourceAsStream("cars.csv"), Iterables.getFirst(mapping,
  // null)
  // .getLogicalSource())
  // .publish()
  // .autoConnect(triplesMappers.size())
  // .log("source");
  //
  // Set<Flux<Statement>> statementFluxes = triplesMappers.stream()
  // .map(triplesMapper -> recordFlux.flatMap(triplesMapper::map)
  // .log(triplesMapper.getTriplesMap()
  // .getResourceName()))
  // .collect(ImmutableSet.toImmutableSet());
  //
  // Flux<Statement> statementFlux = Flux.merge(statementFluxes.toArray(Flux[]::new));
  //
  // statementFlux.subscribe(statement -> System.out.println(String.format("Thread: %s - %s",
  // Thread.currentThread()
  // .getName(), statement)));
  // }
  //
  // @Test
  // public void map_should_work_in_parallel() {
  // Flux<Record> recordFlux = csvResolver.getSourceFlux()
  // .apply(RdfTriplesMapperTest.class.getResourceAsStream("cars.csv"), Iterables.getFirst(mapping,
  // null)
  // .getLogicalSource())
  // .publish()
  // .autoConnect(triplesMappers.size())
  // .log("source");
  //
  // Set<Flux<Statement>> statementFluxes = triplesMappers.stream()
  // .map(triplesMapper -> Flux.from(recordFlux)
  // .parallel()
  // .runOn(Schedulers.parallel())
  // .flatMap(triplesMapper::map)
  // .log(triplesMapper.getTriplesMap()
  // .getResourceName())
  // .sequential())
  // .collect(ImmutableSet.toImmutableSet());
  //
  // Flux<Statement> statementFlux = Flux.merge(statementFluxes.toArray(Flux[]::new));
  //
  // statementFlux.doOnNext(statement -> System.out.println(String.format("Thread: %s - %s",
  // Thread.currentThread()
  // .getName(), statement)))
  // .blockLast();
  // }
  //
  // @Test
  // public void map_should_work_in_parallel2() {
  // ParallelFlux<Record> recordFlux = csvResolver.getSourceFlux()
  // .apply(RdfTriplesMapperTest.class.getResourceAsStream("cars.csv"), Iterables.getFirst(mapping,
  // null)
  // .getLogicalSource())
  // .subscribeOn(Schedulers.boundedElastic())
  // .publish()
  // .autoConnect(triplesMappers.size())
  // .parallel()
  // .runOn(Schedulers.parallel())
  // // .log("source")
  // ;
  //
  // Set<Flux<Statement>> statementFluxes = triplesMappers.stream()
  // .map(triplesMapper -> recordFlux.flatMap(triplesMapper::map)
  // .log(triplesMapper.getTriplesMap()
  // .getResourceName())
  // .sequential())
  // .collect(ImmutableSet.toImmutableSet());
  //
  // Flux<Statement> statementFlux = Flux.merge(statementFluxes.toArray(Flux[]::new));
  //
  // Mono<List<Statement>> modelMono = statementFlux
  // .doOnNext(statement -> System.out.println(String.format("Thread: %s - %s", Thread.currentThread()
  // .getName(), statement)))
  // .collect(Collectors.toList());
  //
  // List<Statement> model = modelMono.block();
  // model.stream()
  // .map(Statement::toString)
  // .sorted()
  // .forEach(System.out::println);
  // // System.out.println(ModelSerializer.serializeAsRdf(model, RDFFormat.TURTLE));
  // }
}
