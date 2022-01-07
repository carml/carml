package com.taxonic.carml.engine.rdf;

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.engine.RefObjectMapper;
import com.taxonic.carml.engine.TriplesMapper;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoin;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinCondition;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStore;
import com.taxonic.carml.engine.reactivedev.join.ChildSideJoinStoreProvider;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStore;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinKey;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.Models;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class RdfRefObjectMapper implements RefObjectMapper<Statement> {

  @Getter(AccessLevel.PUBLIC)
  @NonNull
  private final RefObjectMap refObjectMap;

  @Getter(AccessLevel.PUBLIC)
  @NonNull
  private final TriplesMap triplesMap;

  private final ChildSideJoinStore<Resource, IRI> childSideJoinStore;

  @NonNull
  private final ValueFactory valueFactory;

  public static RdfRefObjectMapper of(@NonNull RefObjectMap refObjectMap, @NonNull TriplesMap triplesMap,
      @NonNull RdfMappingContext rdfMappingContext,
      @NonNull ChildSideJoinStoreProvider<Resource, IRI> childSideJoinStoreProvider) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating mapper for RefObjectMap {}", refObjectMap.getResourceName());
    }

    return new RdfRefObjectMapper(refObjectMap, triplesMap,
        childSideJoinStoreProvider.createChildSideJoinStore(refObjectMap.getId()),
        rdfMappingContext.getValueFactorySupplier()
            .get());
  }

  private RdfRefObjectMapper(@NonNull RefObjectMap refObjectMap, @NonNull TriplesMap triplesMap,
      ChildSideJoinStore<Resource, IRI> childSideJoinStore, @NonNull ValueFactory valueFactory) {
    this.refObjectMap = refObjectMap;
    this.triplesMap = triplesMap;
    this.childSideJoinStore = childSideJoinStore;
    this.valueFactory = valueFactory;
  }

  public void map(Map<Set<Resource>, Set<Resource>> subjectsAndAllGraphs, Set<IRI> predicates,
      ExpressionEvaluation expressionEvaluation) {
    prepareChildSideJoins(subjectsAndAllGraphs, predicates, expressionEvaluation);
  }

  private void prepareChildSideJoins(Map<Set<Resource>, Set<Resource>> subjectsAndAllGraphs, Set<IRI> predicates,
      ExpressionEvaluation expressionEvaluation) {
    Set<ChildSideJoinCondition> childSideJoinConditions = refObjectMap.getJoinConditions()
        .stream()
        .map(joinCondition -> {
          String childReference = joinCondition.getChild();
          ArrayList<String> childValues = expressionEvaluation.apply(joinCondition.getChild())
              .map(expressionResult -> new ArrayList<>(ExpressionEvaluation.extractValues(expressionResult)))
              .orElse(new ArrayList<>());

          return ChildSideJoinCondition.of(childReference, childValues, joinCondition.getParent());
        })
        .collect(Collectors.toSet());

    Set<ChildSideJoin<Resource, IRI>> childSideJoins = subjectsAndAllGraphs.entrySet()
        .stream()
        .map(subjectsAndAllGraphsEntry -> prepareChildSideJoin(subjectsAndAllGraphsEntry.getKey(), predicates,
            subjectsAndAllGraphsEntry.getValue(), childSideJoinConditions))
        .collect(Collectors.toUnmodifiableSet());

    childSideJoinStore.addAll(childSideJoins);
  }

  private ChildSideJoin<Resource, IRI> prepareChildSideJoin(Set<Resource> subjects, Set<IRI> predicates,
      Set<Resource> graphs, Set<ChildSideJoinCondition> childSideJoinConditions) {
    return ChildSideJoin.<Resource, IRI>builder()
        .subjects(new HashSet<>(subjects))
        .predicates(new HashSet<>(predicates))
        .graphs(new HashSet<>(graphs))
        .childSideJoinConditions(new HashSet<>(childSideJoinConditions))
        .build();
  }

  @Override
  public Flux<Statement> resolveJoins(Flux<Statement> mainFlux, TriplesMapper<?, Statement> parentTriplesMapper,
      Flux<Statement> parentFlux) {

    ConnectableFlux<Statement> joinedStatementFlux = childSideJoinStore.clearingFlux()
        .subscribeOn(Schedulers.boundedElastic())
        .flatMap(childSideJoin -> resolveJoin(parentTriplesMapper, childSideJoin))
        .doFinally(signalType -> parentTriplesMapper.notifyCompletion(this, signalType)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe())
        .publish();

    Flux<Statement> barrier = setTriplesMapperCompletionBarrier(joinedStatementFlux, mainFlux, parentFlux);

    return Flux.merge(joinedStatementFlux, barrier);
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
    ParentSideJoinConditionStore<Resource> parentJoinConditions = parentTriplesMapper2.getParentSideJoinConditions();

    Set<Resource> objects = checkJoinAndGetObjects(childSideJoin, parentJoinConditions);

    if (!objects.isEmpty()) {
      Set<Resource> subjects = childSideJoin.getSubjects();
      Set<IRI> predicates = childSideJoin.getPredicates();
      Set<Resource> graphs = childSideJoin.getGraphs();

      return Flux.fromStream(Models.streamCartesianProductStatements(subjects, predicates, objects, graphs,
          RdfTriplesMapper.defaultGraphModifier, valueFactory, RdfTriplesMapper.logAddStatements));
    }

    return Flux.empty();
  }

  private Set<Resource> checkJoinAndGetObjects(ChildSideJoin<Resource, IRI> childSideJoin,
      ParentSideJoinConditionStore<Resource> parentJoinConditions) {

    List<Set<Resource>> parentResults = childSideJoin.getChildSideJoinConditions()
        .stream()
        .map(childSideJoinCondition -> checkChildSideJoinCondition(childSideJoinCondition, parentJoinConditions))
        .collect(Collectors.toList());

    if (parentResults.isEmpty()) {
      return Set.of();
    } else if (parentResults.size() == 1) {
      return parentResults.get(0);
    }

    // return intersection or parentResults
    return parentResults.stream()
        .skip(1)
        .collect(() -> new HashSet<>(parentResults.get(0)), Set::retainAll, Set::retainAll);
  }

  private Set<Resource> checkChildSideJoinCondition(ChildSideJoinCondition childSideJoinCondition,
      ParentSideJoinConditionStore<Resource> parentJoinConditions) {

    return childSideJoinCondition.getChildValues()
        .stream()
        .flatMap(childValue -> checkChildSideJoinConditionChildValue(childSideJoinCondition, childValue,
            parentJoinConditions).stream())
        .collect(Collectors.toUnmodifiableSet());
  }

  private Set<Resource> checkChildSideJoinConditionChildValue(ChildSideJoinCondition childSideJoinCondition,
      String childValue, ParentSideJoinConditionStore<Resource> parentJoinConditions) {
    ParentSideJoinKey parentSideKey = ParentSideJoinKey.of(childSideJoinCondition.getParentReference(), childValue);

    return parentJoinConditions.containsKey(parentSideKey) ? parentJoinConditions.get(parentSideKey) : Set.of();
  }
}
