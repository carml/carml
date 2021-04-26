package com.taxonic.carml.engine.rdf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.RefObjectMapper;
import com.taxonic.carml.engine.TriplesMapper;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoin;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinCondition;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinKey;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.ModelUtil;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
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
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class RdfRefObjectMapper implements RefObjectMapper<Statement> {

  @Getter(AccessLevel.PUBLIC)
  @NonNull
  private final RefObjectMap refObjectMap;

  @NonNull
  private final TriplesMap triplesMap;

  private final Set<ChildSideJoin<Resource, IRI>> childSideJoins;

  @NonNull
  private final ValueFactory valueFactory;

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

  private RdfRefObjectMapper(@NotNull RefObjectMap refObjectMap, @NotNull TriplesMap triplesMap,
      Set<ChildSideJoin<Resource, IRI>> childSideJoins, @NotNull ValueFactory valueFactory) {
    this.refObjectMap = refObjectMap;
    this.triplesMap = triplesMap;
    this.childSideJoins = childSideJoins;
    this.valueFactory = valueFactory;
  }

  public void map(Set<Resource> subjects, Set<IRI> predicates, Set<Resource> graphs,
      ExpressionEvaluation expressionEvaluation) {
    prepareChildSideJoins(subjects, predicates, graphs, expressionEvaluation);
  }

  private void prepareChildSideJoins(Set<Resource> subjects, Set<IRI> predicates, Set<Resource> graphs,
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

    childSideJoins.add(childSideJoin);
  }

  @Override
  public Flux<Statement> resolveJoins(Flux<Statement> mainFlux, TriplesMapper<?, Statement> parentTriplesMapper,
      Flux<Statement> parentFlux) {

    ConnectableFlux<Statement> joinedStatementFlux2 = Flux.using(() -> childSideJoins, Flux::fromIterable, Set::clear)
        .subscribeOn(Schedulers.boundedElastic())
        .flatMap(childSideJoin -> resolveJoin(parentTriplesMapper, childSideJoin))
        .doFinally(signalType -> parentTriplesMapper.notifyCompletion(this, signalType)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe())
        .publish();

    Flux<Statement> barrier = setTriplesMapperCompletionBarrier(joinedStatementFlux2, mainFlux, parentFlux);

    return Flux.merge(joinedStatementFlux2, barrier);
  }

  private Flux<Statement> setTriplesMapperCompletionBarrier(ConnectableFlux<Statement> joinedStatementFlux,
      Flux<Statement> mainFlux, Flux<Statement> parentFlux) {
    return Mono.fromRunnable(() -> {
      Phaser phaser = new Phaser(1);
      mainFlux.doOnSubscribe(subscription -> phaser.register())
          .doFinally(signalType -> phaser.arriveAndDeregister())
          .subscribe();
      parentFlux.doOnSubscribe(subscription -> phaser.register())
          .doFinally(signalType -> phaser.arriveAndDeregister())
          .subscribe();

      phaser.arriveAndAwaitAdvance();
      joinedStatementFlux.connect();
    })
        .subscribeOn(Schedulers.boundedElastic())
        .thenMany(Flux.empty());
  }

  private Flux<Statement> resolveJoin(TriplesMapper<?, Statement> parentTriplesMapper2,
      ChildSideJoin<Resource, IRI> childSideJoin) {
    Map<ParentSideJoinKey, Set<Resource>> parentJoinConditions = parentTriplesMapper2.getParentSideJoinConditions();

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
