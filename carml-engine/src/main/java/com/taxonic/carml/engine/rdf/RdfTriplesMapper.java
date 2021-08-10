package com.taxonic.carml.engine.rdf;

import static com.taxonic.carml.util.LogUtil.exception;
import static com.taxonic.carml.util.LogUtil.log;

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.RefObjectMapper;
import com.taxonic.carml.engine.TermGenerator;
import com.taxonic.carml.engine.TriplesMapper;
import com.taxonic.carml.engine.TriplesMapperException;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinKey;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.vocab.Rdf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter(AccessLevel.PACKAGE)
public class RdfTriplesMapper<I> implements TriplesMapper<I, Statement> {

  static UnaryOperator<Resource> defaultGraphModifier = graph -> graph.equals(Rdf.Rr.defaultGraph) ? null : graph;

  static Consumer<Statement> logAddStatements = statement -> {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Adding statement {} {} {} {} to result set", statement.getSubject(), statement.getPredicate(),
          statement.getObject(), statement.getContext());
    }
  };

  @NonNull
  @Getter(AccessLevel.PUBLIC)
  private final TriplesMap triplesMap;

  private final Set<RdfSubjectMapper> subjectMappers;

  private final Set<RdfPredicateObjectMapper> predicateObjectMappers;

  private final Set<RdfRefObjectMapper> incomingRefObjectMappers;

  @NonNull
  private final LogicalSourceResolver.ExpressionEvaluationFactory<I> expressionEvaluationFactory;

  @NonNull
  private final RdfMappingContext rdfMappingContext;

  @NonNull
  @Getter(AccessLevel.PUBLIC)
  private final ConcurrentMap<ParentSideJoinKey, Set<Resource>> parentSideJoinConditions;

  private final Map<RefObjectMapper<Statement>, Boolean> incomingRefObjectMapperStatus;

  public static <I> RdfTriplesMapper<I> of(@NonNull TriplesMap triplesMap, Set<RdfRefObjectMapper> refObjectMappers,
      Set<RdfRefObjectMapper> incomingRefObjectMappers,
      @NonNull LogicalSourceResolver.ExpressionEvaluationFactory<I> expressionEvaluatorFactory,
      @NonNull RdfMappingContext rdfMappingContext,
      @NonNull ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating mapper for TriplesMap {}", triplesMap.getResourceName());
    }

    Set<RdfSubjectMapper> subjectMappers = createSubjectMappers(triplesMap, rdfMappingContext);

    Set<RdfPredicateObjectMapper> predicateObjectMappers =
        createPredicateObjectMappers(triplesMap, rdfMappingContext, refObjectMappers);

    Map<RefObjectMapper<Statement>, Boolean> connectedRefObjectMapperStatus = incomingRefObjectMappers.stream()
        .collect(Collectors.toMap(rom -> rom, rom -> false));

    return new RdfTriplesMapper<>(triplesMap, subjectMappers, predicateObjectMappers, incomingRefObjectMappers,
        expressionEvaluatorFactory, rdfMappingContext, parentSideJoinConditionStoreProvider.create(triplesMap.getId()),
        connectedRefObjectMapperStatus);
  }

  static Set<TermGenerator<Resource>> createGraphGenerators(Set<GraphMap> graphMaps,
      RdfTermGeneratorFactory termGeneratorFactory) {
    return graphMaps.stream()
        .map(termGeneratorFactory::getGraphGenerator)
        .collect(Collectors.toUnmodifiableSet());
  }

  private static Set<RdfSubjectMapper> createSubjectMappers(TriplesMap triplesMap,
      RdfMappingContext rdfMappingContext) {

    Set<SubjectMap> subjectMaps = triplesMap.getSubjectMaps();
    if (subjectMaps.isEmpty()) {
      throw new TriplesMapperException(
          String.format("Subject map must be specified in triples map %s", exception(triplesMap, triplesMap)));
    }

    return subjectMaps.stream()
        .peek(sm -> LOG.debug("Creating mapper for SubjectMap {}", sm.getResourceName()))
        .map(sm -> RdfSubjectMapper.of(sm, triplesMap, rdfMappingContext))
        .collect(Collectors.toUnmodifiableSet());
  }

  private static Set<RdfPredicateObjectMapper> createPredicateObjectMappers(TriplesMap triplesMap,
      RdfMappingContext rdfMappingContext, Set<RdfRefObjectMapper> refObjectMappers) {
    return triplesMap.getPredicateObjectMaps()
        .stream()
        .peek(pom -> LOG.debug("Creating mapper for PredicateObjectMap {}", pom.getResourceName()))
        .map(pom -> RdfPredicateObjectMapper.of(pom, triplesMap, refObjectMappers, rdfMappingContext))
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public Set<RdfRefObjectMapper> getRefObjectMappers() {
    return predicateObjectMappers.stream()
        .flatMap(pom -> pom.getRdfRefObjectMappers()
            .stream())
        .collect(Collectors.toUnmodifiableSet());
  }

  Set<RdfRefObjectMapper> getConnectedRefObjectMappers() {
    return Stream.concat(getRefObjectMappers().stream(), incomingRefObjectMappers.stream())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public Flux<Statement> map(I item) {
    LOG.trace("Mapping triples for item {}", item);
    ExpressionEvaluation expressionEvaluation = expressionEvaluationFactory.apply(item);

    return map(expressionEvaluation);
  }

  @Override
  public Flux<Statement> map(ExpressionEvaluation expressionEvaluation) {

    Set<RdfSubjectMapper.Result> subjectMapperResults = subjectMappers.stream()
        .map(subjectMapper -> subjectMapper.map(expressionEvaluation))
        .collect(Collectors.toUnmodifiableSet());

    Set<Resource> subjects = subjectMapperResults.stream()
        .map(RdfSubjectMapper.Result::getSubjects)
        .flatMap(Set::stream)
        .collect(Collectors.toUnmodifiableSet());

    if (subjects.isEmpty()) {
      return Flux.empty();
    }

    Map<Set<Resource>, Set<Resource>> subjectsAndSubjectGraphs = new HashMap<>();
    List<Flux<Statement>> subjectStatementFluxes = new ArrayList<>();

    for (RdfSubjectMapper.Result subjectMapperResult : subjectMapperResults) {
      Set<Resource> resultSubjects = subjectMapperResult.getSubjects();
      if (!resultSubjects.isEmpty()) {
        subjectsAndSubjectGraphs.put(resultSubjects, subjectMapperResult.getGraphs());
        subjectStatementFluxes.add(subjectMapperResult.getTypeStatements());
      }
    }

    Flux<Statement> subjectStatements = Flux.merge(subjectStatementFluxes);
    Flux<Statement> pomStatements = Flux.fromIterable(predicateObjectMappers)
        .flatMap(predicateObjectMapper -> predicateObjectMapper.map(expressionEvaluation, subjectsAndSubjectGraphs));

    cacheParentSideJoinConditions(expressionEvaluation, subjects);

    return Flux.merge(subjectStatements, pomStatements);
  }

  private void cacheParentSideJoinConditions(ExpressionEvaluation expressionEvaluation, Set<Resource> subjects) {
    incomingRefObjectMappers.forEach(incomingRefObjectMapper -> incomingRefObjectMapper.getRefObjectMap()
        .getJoinConditions()
        .forEach(join -> processJoinCondition(join, expressionEvaluation, subjects)));
  }

  private void processJoinCondition(Join join, ExpressionEvaluation expressionEvaluation, Set<Resource> subjects) {
    String parentReference = join.getParentReference();

    expressionEvaluation.apply(parentReference)
        .ifPresent(referenceResult -> ExpressionEvaluation.extractValues(referenceResult)
            .forEach(parentValue -> processJoinConditionParentValue(subjects, parentReference, parentValue)));
  }

  private void processJoinConditionParentValue(Set<Resource> subjects, String parentReference, String parentValue) {
    ParentSideJoinKey parentSideJoinKey = ParentSideJoinKey.of(parentReference, parentValue);
    Set<Resource> parentSubjects = new HashSet<>(subjects);

    if (parentSideJoinConditions.containsKey(parentSideJoinKey)) {
      // merge incoming subjects with already cached subjects for key
      parentSubjects.addAll(parentSideJoinConditions.get(parentSideJoinKey));
    }

    parentSideJoinConditions.put(ParentSideJoinKey.of(parentReference, parentValue), parentSubjects);
  }

  @Override
  public Mono<Void> notifyCompletion(RefObjectMapper<Statement> refObjectMapper, SignalType signalType) {
    if (!signalType.equals(SignalType.ON_COMPLETE)) {
      throw new TriplesMapperException(String.format(
          "Provided refObjectMapper for %n%s%n notifying completion with unsupported signal `%s` TriplesMapper %n%s",
          exception(refObjectMapper.getRefObjectMap()), signalType, log(triplesMap)));
    }

    if (!incomingRefObjectMapperStatus.containsKey(refObjectMapper)) {
      throw new TriplesMapperException(
          String.format("Provided refObjectMap(per) for%n%s%n is not known to be connected to TriplesMap(per)%n%s",
              exception(refObjectMapper.getRefObjectMap()), log(triplesMap)));
    }

    incomingRefObjectMapperStatus.put(refObjectMapper, true);

    boolean incomingRefObjectMappersDone = incomingRefObjectMapperStatus.values()
        .stream()
        .allMatch(Boolean::valueOf);

    if (incomingRefObjectMappersDone) {
      return cleanup();
    }

    return Mono.empty();
  }

  private Mono<Void> cleanup() {
    return Mono.fromRunnable(parentSideJoinConditions::clear)
        .subscribeOn(Schedulers.boundedElastic())
        .then();
  }

}
