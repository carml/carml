package com.taxonic.carml.engine.rdf;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.LogicalSourcePipeline;
import com.taxonic.carml.engine.TriplesMapper;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.TriplesMap;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private final Set<RdfTriplesMapper<I>> triplesMappers;

  public static <I> RdfLogicalSourcePipeline<I> of(@NonNull LogicalSource logicalSource, List<TriplesMap> triplesMaps,
      Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers, Map<RdfRefObjectMapper, TriplesMap> roMapperToParentTm,
      LogicalSourceResolver<I> logicalSourceResolver, RdfMappingContext rdfMappingContext,
      ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider) {

    Set<RdfTriplesMapper<I>> triplesMappers = triplesMaps.stream()
        .map(triplesMap -> constructTriplesMapper(triplesMap, tmToRoMappers, roMapperToParentTm, logicalSourceResolver,
            rdfMappingContext, parentSideJoinConditionStoreProvider))
        .collect(ImmutableSet.toImmutableSet());

    return of(logicalSource, logicalSourceResolver, rdfMappingContext, triplesMappers);
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
        .collect(ImmutableSet.toImmutableSet());

    return RdfTriplesMapper.of(triplesMap, roMappers, incomingRoMappers,
        logicalSourceResolver.getExpressionEvaluationFactory(), rdfMappingContext,
        parentSideJoinConditionStoreProvider);
  }

  public Map<TriplesMapper<I, Statement>, Flux<Statement>> run() {
    return run(Set.of());
  }

  public Map<TriplesMapper<I, Statement>, Flux<Statement>> run(Set<TriplesMap> triplesMapFilter) {
    return run(null, triplesMapFilter);
  }

  public Map<TriplesMapper<I, Statement>, Flux<Statement>> run(InputStream source) {
    return run(source, Set.of());
  }

  public Map<TriplesMapper<I, Statement>, Flux<Statement>> run(InputStream source,
      Set<TriplesMap> triplesMapFilter) {
    boolean filterEmpty = triplesMapFilter == null || triplesMapFilter.isEmpty();
    int nrOfTriplesMappers = filterEmpty ? triplesMappers.size() : triplesMapFilter.size();

    Flux<I> itemFlux = logicalSourceResolver.getSourceFlux()
        .apply(source, logicalSource)
        .subscribeOn(Schedulers.boundedElastic())
        .publish()
        .autoConnect(nrOfTriplesMappers);

    return triplesMappers.stream()
        .filter(triplesMapper -> filterEmpty || triplesMapFilter.contains(triplesMapper.getTriplesMap()))
        .collect(ImmutableMap.toImmutableMap(triplesMapper -> triplesMapper,
            triplesMapper -> itemFlux.flatMap(triplesMapper::map)
                .publish()
                // wait for all subscribers to be ready
                .autoConnect(1 + triplesMapper.getConnectedRefObjectMappers()
                    .size())));
  }
}
