package com.taxonic.carml.engine.rdf;

import static com.taxonic.carml.util.LogUtil.exception;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.engine.LogicalSourcePipeline;
import com.taxonic.carml.engine.TriplesMapperException;
import com.taxonic.carml.engine.reactivedev.join.ParentSideJoinConditionStoreProvider;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.TriplesMap;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class RdfLogicalSourcePipeline<I> implements LogicalSourcePipeline<I, Statement> {
  private final LogicalSource logicalSource;

  private final LogicalSourceResolver<I> logicalSourceResolver;

  private final RdfMappingContext rdfMappingContext;

  private final Set<RdfTriplesMapper<I>> triplesMappers;

  // TODO: Builder?
  public static <I> RdfLogicalSourcePipeline<I> of(@NonNull LogicalSource logicalSource, List<TriplesMap> triplesMaps,
      Map<TriplesMap, Set<RdfRefObjectMapper>> tmToRoMappers, Map<RdfRefObjectMapper, TriplesMap> roMapperToParentTm,
      LogicalSourceResolver<I> logicalSourceResolver, RdfMappingContext rdfMappingContext,
      ParentSideJoinConditionStoreProvider<Resource> parentSideJoinConditionStoreProvider) {

    Set<RdfTriplesMapper<I>> triplesMappers = triplesMaps.stream()
        .map(triplesMap -> {
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
        })
        .collect(ImmutableSet.toImmutableSet());

    return of(logicalSource, triplesMappers, logicalSourceResolver, rdfMappingContext);
  }

  public static <I> RdfLogicalSourcePipeline<I> of(LogicalSource logicalSource, Set<RdfTriplesMapper<I>> triplesMappers,
      LogicalSourceResolver<I> logicalSourceResolver, RdfMappingContext rdfMappingContext) {
    return new RdfLogicalSourcePipeline<>(logicalSource, logicalSourceResolver, rdfMappingContext, triplesMappers);
  }

  public Map<TriplesMap, Flux<Statement>> run() {
    return run(null);
  }

  public Map<TriplesMap, Flux<Statement>> run(InputStream source) {
    // TODO: why does execution sometimes hang when ParallelFlux used?
    // ParallelFlux<I> itemFlux = logicalSourceResolver.getSourceFlux()
    Flux<I> itemFlux = logicalSourceResolver.getSourceFlux()
        .apply(source, logicalSource)
        .subscribeOn(Schedulers.boundedElastic())
        .publish()
        .autoConnect(triplesMappers.size());
    // .parallel()
    // .runOn(Schedulers.parallel());

    Map<TriplesMap, Flux<Statement>> mappingResults = new HashMap<>();

    for (RdfTriplesMapper<I> triplesMapper : triplesMappers) {
      Flux<Statement> joinlessTriplesFlux = itemFlux.flatMap(triplesMapper::map)
          .doOnComplete(() -> triplesMapper.streamConnectedRefObjectMappers()
              .forEach(rdfRefObjectMapper -> rdfRefObjectMapper.signalCompletion(triplesMapper)));
      // .sequential()

      // Signal done to connected RefObjectMappers
      // Flux<Void> tmCompletion = Flux.fromStream(triplesMapper.streamConnectedRefObjectMappers())
      // .map(rdfRefObjectMapper -> rdfRefObjectMapper.signalCompletion(triplesMapper))
      // .flatMap(voidMono -> voidMono);
      // joinlessTriplesFlux.thenEmpty(tmCompletion)
      // .subscribeOn(Schedulers.boundedElastic())
      // .subscribe();

      Flux<Statement> joinedTriplesFlux = Flux.merge(triplesMapper.streamRefObjectMappers()
          .map(RdfRefObjectMapper::resolveJoins)
          .collect(ImmutableList.toImmutableList()));

      Flux<Statement> mappedTriplesFlux = Flux.merge(joinlessTriplesFlux, joinedTriplesFlux)
          .onErrorResume(error -> Flux.error(new TriplesMapperException(
              String.format("Something went wrong for TriplesMap %s", exception(triplesMapper.getTriplesMap())),
              error)));

      mappingResults.put(triplesMapper.getTriplesMap(), mappedTriplesFlux);
    }

    return mappingResults;
  }

}
