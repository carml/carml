package com.taxonic.carml.engine.rdf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoin;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinCondition;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinKey;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.LogUtil;
import com.taxonic.carml.util.ModelUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class RdfRefObjectMapper {

  @Getter(AccessLevel.PACKAGE)
  @NonNull
  private final RefObjectMap refObjectMap;

  @NonNull
  private final TriplesMap triplesMap;

  private final Set<ChildSideJoin<Resource, IRI>> childSideJoins;

  private final Map<RdfTriplesMapper<?>, Boolean> triplesMapperStatus;

  @NonNull
  private final ValueFactory valueFactory;

  private RdfTriplesMapper<?> parentTriplesMapper;

  private ConnectableFlux<Statement> joinedStatementFlux;

  public static RdfRefObjectMapper of(@NonNull RefObjectMap refObjectMap, @NonNull TriplesMap triplesMap,
      @NonNull RdfMappingContext rdfMappingContext,
      @NonNull ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating mapper for RefObjectMap {}", refObjectMap.getResourceName());
    }

    return new RdfRefObjectMapper(refObjectMap, triplesMap, childSideJoinStoreProvider.create(refObjectMap.getId()),
        rdfMappingContext.getValueFactorySupplier()
            .get());
  }

  private RdfRefObjectMapper(RefObjectMap refObjectMap, TriplesMap triplesMap,
      Set<ChildSideJoin<Resource, IRI>> childSideJoins, ValueFactory valueFactory) {
    this.refObjectMap = refObjectMap;
    this.triplesMap = triplesMap;
    this.childSideJoins = childSideJoins;
    this.triplesMapperStatus = new HashMap<>();
    this.valueFactory = valueFactory;
  }

  public Mono<Statement> map(Set<Resource> subjects, Set<IRI> predicates, Set<Resource> graphs,
      ExpressionEvaluation expressionEvaluation) {
    return prepareChildSideJoins(subjects, predicates, graphs, expressionEvaluation).flatMap(v -> v);
  }

  private Mono<Mono<Statement>> prepareChildSideJoins(Set<Resource> subjects, Set<IRI> predicates, Set<Resource> graphs,
      ExpressionEvaluation expressionEvaluation) {
    Set<ChildSideJoinCondition> childSideJoinConditions = refObjectMap.getJoinConditions()
        .stream()
        .map(joinCondition -> {
          String childReference = joinCondition.getChildReference();
          List<String> childValues = expressionEvaluation.apply(joinCondition.getChildReference())
              .map(ExpressionEvaluation::extractValues)
              .orElse(ImmutableList.of());

          return ChildSideJoinCondition.of(childReference, childValues, joinCondition.getParentReference());
        })
        .collect(Collectors.toSet());

    ChildSideJoin<Resource, IRI> childSideJoin = ChildSideJoin.<Resource, IRI>builder()
        .subjects(subjects)
        .predicates(predicates)
        .graphs(graphs)
        .childSideJoinConditions(childSideJoinConditions)
        .build();

    return Mono.fromCallable(() -> childSideJoins.add(childSideJoin))
        .subscribeOn(Schedulers.boundedElastic())
        .thenReturn(Mono.empty()); // We're only interested in completion of this mono.
  }

  public Mono<Void> signalCompletion(RdfTriplesMapper<?> triplesMapper) {
    TriplesMap providedTriplesMap = triplesMapper.getTriplesMap();
    boolean isParentTriplesMapper = providedTriplesMap.equals(refObjectMap.getParentTriplesMap());

    if (!providedTriplesMap.equals(triplesMap) && !isParentTriplesMapper) {
      throw new IllegalStateException(String.format(
          "RefObjectMapper only supports triples mappers on the triples map that references it, and the parent "
              + "triples map it references. The provided triplesMap is not supported: %s",
          LogUtil.exception(providedTriplesMap)));
    }

    if (isParentTriplesMapper) {
      this.parentTriplesMapper = triplesMapper;
    }

    if (triplesMapperStatus.containsKey(triplesMapper)) {
      throw new IllegalStateException(
          String.format("TriplesMapper %s for triplesMap %s signaled completion multiple times.", triplesMapper,
              triplesMapper.getTriplesMap()
                  .getResourceName()));
    }

    triplesMapperStatus.put(triplesMapper, true);

    boolean ready = triplesMapperStatus.size() == 2 && triplesMapperStatus.values()
        .stream()
        .allMatch(Boolean::valueOf);

    if (ready) {
      joinedStatementFlux.connect();
    }

    return Mono.empty();
  }

  public ConnectableFlux<Statement> resolveJoins() {
    joinedStatementFlux = Flux.using(() -> childSideJoins, Flux::fromIterable, Set::clear)
        .subscribeOn(Schedulers.boundedElastic())
        // .parallel()
        // .runOn(Schedulers.parallel())
        .flatMap(this::resolveJoin)
        // .sequential()
        .doFinally(signalType -> parentTriplesMapper.notifyCompletion(this, signalType)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe())
        .publish();

    return joinedStatementFlux;
    // if both complete,
    // iterate over ChildSideJoins in ChildSideJoinCache
    // for each childSideJoin
    // check all childSideJoinConditions:
    // get corresponding parentSideJoinCondition using parentTriplesMapID and parentExpression
    // check if all childSideJoinConditions have a match in parentSideJoinCondition:
    // for all childSideJoinConditions
    // if one of the childValues matches with one of the parentValues then the condition matches
  }

  private Flux<Statement> resolveJoin(ChildSideJoin<Resource, IRI> childSideJoin) {
    Map<ParentSideJoinKey, Set<Resource>> parentJoinConditions = parentTriplesMapper.getParentSideJoinConditions();

    Set<Resource> objects = checkJoinAndGetObjects(childSideJoin, parentJoinConditions);

    if (!objects.isEmpty()) {
      Set<Resource> subjects = childSideJoin.getSubjects();
      Set<IRI> predicates = childSideJoin.getPredicates();
      Set<Resource> graphs = childSideJoin.getGraphs();

      return Flux.fromStream(ModelUtil.streamCartesianProductStatements(subjects, predicates, objects, graphs,
          RdfTriplesMapper.defaultGraphModifier, valueFactory, RdfTriplesMapper.logAddStatements));
    }

    return Flux.empty();
  }

  @SuppressWarnings("unchecked")
  private Set<Resource> checkJoinAndGetObjects(ChildSideJoin<Resource, IRI> childSideJoin,
      Map<ParentSideJoinKey, Set<Resource>> parentJoinConditions) {

    List<Set<Resource>> parentResults = childSideJoin.getChildSideJoinConditions()
        .stream()
        .map(childSideJoinCondition -> checkChildSideJoinCondition(childSideJoinCondition, parentJoinConditions))
        .collect(Collectors.toList());

    if (parentResults.isEmpty()) {
      return ImmutableSet.of();
    } else if (parentResults.size() == 1) {
      return parentResults.get(0);
    }

    return Sets.intersectAll(parentResults.toArray(Set[]::new));
  }

  private Set<Resource> checkChildSideJoinCondition(ChildSideJoinCondition childSideJoinCondition,
      Map<ParentSideJoinKey, Set<Resource>> parentJoinConditions) {

    return childSideJoinCondition.getChildValues()
        .stream()
        .flatMap(childValue -> checkChildSideJoinConditionChildValue(childSideJoinCondition, childValue,
            parentJoinConditions).stream())
        .collect(ImmutableSet.toImmutableSet());
  }

  private Set<Resource> checkChildSideJoinConditionChildValue(ChildSideJoinCondition childSideJoinCondition,
      String childValue, Map<ParentSideJoinKey, Set<Resource>> parentJoinConditions) {
    ParentSideJoinKey parentSideKey = ParentSideJoinKey.of(childSideJoinCondition.getParentReference(), childValue);

    return parentJoinConditions.containsKey(parentSideKey) ? parentJoinConditions.get(parentSideKey)
        : ImmutableSet.of();
  }

}
