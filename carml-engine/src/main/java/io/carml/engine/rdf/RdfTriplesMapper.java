package io.carml.engine.rdf;

import static io.carml.util.LogUtil.exception;

import io.carml.engine.ExpressionEvaluation;
import io.carml.engine.RefObjectMapper;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapper;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.engine.join.ParentSideJoinConditionStoreProvider;
import io.carml.engine.join.ParentSideJoinKey;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.model.GraphMap;
import io.carml.model.Join;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RdfTriplesMapper<R> implements TriplesMapper<Statement> {

  static UnaryOperator<Resource> defaultGraphModifier = graph -> graph.equals(Rdf.Rr.defaultGraph) ? null : graph;

  static Consumer<Statement> logAddStatements = statement -> {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Adding statement {} {} {} {} to result set", statement.getSubject(), statement.getPredicate(),
          statement.getObject(), statement.getContext());
    }
  };

  @NonNull
  private final TriplesMap triplesMap;

  private final Set<RdfSubjectMapper> subjectMappers;

  private final Set<RdfPredicateObjectMapper> predicateObjectMappers;

  private final Set<RdfRefObjectMapper> incomingRefObjectMappers;

  @NonNull
  private final LogicalSourceResolver.ExpressionEvaluationFactory<R> expressionEvaluationFactory;

  @NonNull
  private final RdfMapperConfig rdfMapperConfig;

  @NonNull
  private final ParentSideJoinConditionStore<Resource> parentSideJoinConditions;

  public static <I> RdfTriplesMapper<I> of(@NonNull TriplesMap triplesMap, Set<RdfRefObjectMapper> refObjectMappers,
      Set<RdfRefObjectMapper> incomingRefObjectMappers,
      @NonNull LogicalSourceResolver.ExpressionEvaluationFactory<I> expressionEvaluatorFactory,
      @NonNull RdfMapperConfig rdfMapperConfig,
      @NonNull ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating mapper for TriplesMap {}", triplesMap.getResourceName());
    }

    Set<RdfSubjectMapper> subjectMappers = createSubjectMappers(triplesMap, rdfMapperConfig);

    Set<RdfPredicateObjectMapper> predicateObjectMappers =
        createPredicateObjectMappers(triplesMap, rdfMapperConfig, refObjectMappers);

    return new RdfTriplesMapper<>(triplesMap, subjectMappers, predicateObjectMappers, incomingRefObjectMappers,
        expressionEvaluatorFactory, rdfMapperConfig,
        parentSideJoinConditionStoreProvider.createParentSideJoinConditionStore(triplesMap.getId()));
  }

  static Set<TermGenerator<Resource>> createGraphGenerators(Set<GraphMap> graphMaps,
      RdfTermGeneratorFactory termGeneratorFactory) {
    return graphMaps.stream()
        .map(termGeneratorFactory::getGraphGenerator)
        .collect(Collectors.toUnmodifiableSet());
  }

  @SuppressWarnings("java:S3864")
  private static Set<RdfSubjectMapper> createSubjectMappers(TriplesMap triplesMap, RdfMapperConfig rdfMapperConfig) {

    Set<SubjectMap> subjectMaps = triplesMap.getSubjectMaps();
    if (subjectMaps.isEmpty()) {
      throw new TriplesMapperException(
          String.format("Subject map must be specified in triples map %s", exception(triplesMap, triplesMap)));
    }

    return subjectMaps.stream()
        .peek(sm -> LOG.debug("Creating mapper for SubjectMap {}", sm.getResourceName()))
        .map(sm -> RdfSubjectMapper.of(sm, triplesMap, rdfMapperConfig))
        .collect(Collectors.toUnmodifiableSet());
  }

  @SuppressWarnings("java:S3864")
  private static Set<RdfPredicateObjectMapper> createPredicateObjectMappers(TriplesMap triplesMap,
      RdfMapperConfig rdfMapperConfig, Set<RdfRefObjectMapper> refObjectMappers) {
    return triplesMap.getPredicateObjectMaps()
        .stream()
        .peek(pom -> LOG.debug("Creating mapper for PredicateObjectMap {}", pom.getResourceName()))
        .map(pom -> RdfPredicateObjectMapper.of(pom, triplesMap, refObjectMappers, rdfMapperConfig))
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public TriplesMap getTriplesMap() {
    return triplesMap;
  }

  @Override
  public Set<RefObjectMapper<Statement>> getRefObjectMappers() {
    return predicateObjectMappers.stream()
        .flatMap(pom -> pom.getRdfRefObjectMappers()
            .stream())
        .collect(Collectors.toUnmodifiableSet());
  }

  Set<RefObjectMapper<Statement>> getConnectedRefObjectMappers() {
    return Stream.concat(getRefObjectMappers().stream(), incomingRefObjectMappers.stream())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public ParentSideJoinConditionStore<Resource> getParentSideJoinConditions() {
    return parentSideJoinConditions;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Flux<Statement> map(LogicalSourceRecord<?> logicalSourceRecord) {
    var lsRecord = (R) logicalSourceRecord.getRecord();
    LOG.trace("Mapping triples for record {}", logicalSourceRecord);
    ExpressionEvaluation expressionEvaluation = expressionEvaluationFactory.apply(lsRecord);

    return mapEvaluation(expressionEvaluation);
  }

  @Override
  public Flux<Statement> mapEvaluation(ExpressionEvaluation expressionEvaluation) {

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
    String parentReference = join.getParent();

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

  public void cleanup() {
    parentSideJoinConditions.clear();
  }
}
