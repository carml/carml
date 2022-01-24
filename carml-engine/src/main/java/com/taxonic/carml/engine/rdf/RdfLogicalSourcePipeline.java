package com.taxonic.carml.engine.rdf;

import com.taxonic.carml.engine.LogicalSourcePipeline;
import com.taxonic.carml.engine.TriplesMapper;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.logicalsourceresolver.LogicalSourceResolver;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.TriplesMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@RequiredArgsConstructor(staticName = "of")
@Getter
public class RdfLogicalSourcePipeline<I> implements LogicalSourcePipeline<I, Statement> {

  @NonNull
  private final LogicalSource logicalSource;

  @NonNull
  private final LogicalSourceResolver<I> logicalSourceResolver;

  @NonNull
  private final RdfMappingContext rdfMappingContext;

  private final List<TriplesMap> triplesMaps;

  private final Set<RdfTriplesMapper<I>> triplesMappers;

  public static <I> RdfLogicalSourcePipeline<I> of(@NonNull LogicalSource logicalSource, List<TriplesMap> triplesMaps,
      Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers, Map<RdfRefObjectMapper, TriplesMap> roMapperToParentTm,
      LogicalSourceResolver<I> logicalSourceResolver, RdfMappingContext rdfMappingContext,
      ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider) {

    Set<RdfTriplesMapper<I>> triplesMappers = triplesMaps.stream()
        .map(triplesMap -> constructTriplesMapper(triplesMap, tmToRoMappers, roMapperToParentTm, logicalSourceResolver,
            rdfMappingContext, parentSideJoinConditionStoreProvider))
        .collect(Collectors.toUnmodifiableSet());

    return of(logicalSource, logicalSourceResolver, rdfMappingContext, triplesMaps, triplesMappers);
  }

  private static <I> RdfTriplesMapper<I> constructTriplesMapper(TriplesMap triplesMap,
      Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers, Map<RdfRefObjectMapper, TriplesMap> roMapperToParentTm,
      LogicalSourceResolver<I> logicalSourceResolver, RdfMappingContext rdfMappingContext,
      ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider) {
    Set<RdfRefObjectMapper> roMappers = tmToRoMappers.get(triplesMap);
    Set<RdfRefObjectMapper> incomingRoMappers = roMapperToParentTm.entrySet()
        .stream()
        .filter(entry -> entry.getValue()
            .equals(triplesMap))
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableSet());

    return RdfTriplesMapper.of(triplesMap, roMappers, incomingRoMappers,
        logicalSourceResolver.getExpressionEvaluationFactory(), rdfMappingContext,
        parentSideJoinConditionStoreProvider);
  }

  @Override
  public LogicalSource getLogicalSource() {
    return logicalSource;
  }

  public Map<TriplesMapper<I, Statement>, Flux<Statement>> run() {
    return run(Set.of());
  }

  @Override
  public Map<TriplesMapper<I, Statement>, Flux<Statement>> run(Set<TriplesMap> triplesMapFilter) {
    return run(null, triplesMapFilter);
  }

  public Map<TriplesMapper<I, Statement>, Flux<Statement>> run(Object source) {
    return run(source, Set.of());
  }

  @Override
  public Map<TriplesMapper<I, Statement>, Flux<Statement>> run(Object source, Set<TriplesMap> triplesMapFilter) {
    var actionableTriplesMappers = filterTriplesMappers(triplesMapFilter);

    Flux<I> itemFlux = logicalSourceResolver.getSourceFlux()
        .apply(source, logicalSource)
        .subscribeOn(Schedulers.boundedElastic())
        .publish()
        .autoConnect(actionableTriplesMappers.size());

    return actionableTriplesMappers.stream()
        .collect(Collectors.toUnmodifiableMap(triplesMapper -> triplesMapper,
            triplesMapper -> itemFlux.flatMap(triplesMapper::map)
                .publish()
                // wait for all subscribers to be ready
                .autoConnect(1 + triplesMapper.getConnectedRefObjectMappers()
                    .size())));
  }

  private Set<RdfTriplesMapper<I>> filterTriplesMappers(Set<TriplesMap> triplesMapFilter) {
    boolean filterEmpty = triplesMapFilter == null || triplesMapFilter.isEmpty();
    return triplesMappers.stream()
        .filter(triplesMapper -> filterEmpty || triplesMapFilter.contains(triplesMapper.getTriplesMap()))
        .collect(Collectors.toUnmodifiableSet());
  }
}
