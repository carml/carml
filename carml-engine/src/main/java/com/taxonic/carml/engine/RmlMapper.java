package com.taxonic.carml.engine;

import static com.taxonic.carml.util.LogUtil.exception;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.ReactiveInputStreams;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public abstract class RmlMapper<T> {

  private static final String DEFAULT_STREAM_NAME = "DEFAULT";

  private Function<Object, Optional<Flux<DataBuffer>>> sourceResolver;

  private Map<TriplesMap, LogicalSourcePipeline<?, T>> logicalSourcePipelinePool;

  private Map<? extends RefObjectMapper<T>, TriplesMap> refObjectMapperToParentTriplesMap;

  public Flux<T> map() {
    return map(Map.of());
  }

  public Flux<T> map(Set<TriplesMap> triplesMapFilter) {
    return map(Map.of(), triplesMapFilter);
  }

  public Flux<T> map(@NonNull InputStream inputStream) {
    return map(Map.of(DEFAULT_STREAM_NAME, inputStream));
  }

  public Flux<T> map(@NonNull InputStream inputStream, Set<TriplesMap> triplesMapFilter) {
    return map(Map.of(DEFAULT_STREAM_NAME, inputStream), triplesMapFilter);
  }

  public Flux<T> map(Map<String, InputStream> namedInputStreams) {
    return map(namedInputStreams, Set.of());
  }

  public Flux<T> map(Map<String, InputStream> namedInputStreams, Set<TriplesMap> triplesMapFilter) {
    return Flux.merge(mapPerTriplesMap(namedInputStreams, triplesMapFilter).values());
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap() {
    return mapPerTriplesMap(Map.of());
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap(Set<TriplesMap> triplesMapFilter) {
    return mapPerTriplesMap(Map.of(), triplesMapFilter);
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap(@NonNull InputStream inputStream) {
    return mapPerTriplesMap(Map.of(DEFAULT_STREAM_NAME, inputStream));
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap(@NonNull InputStream inputStream, Set<TriplesMap> triplesMapFilter) {
    return mapPerTriplesMap(Map.of(DEFAULT_STREAM_NAME, inputStream), triplesMapFilter);
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap(Map<String, InputStream> namedInputStreams) {
    return mapPerTriplesMap(namedInputStreams, Set.of());
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap(Map<String, InputStream> namedInputStreams,
      Set<TriplesMap> triplesMapFilter) {
    Map<Object, List<LogicalSourcePipeline<?, T>>> groupedLogicalSourcePipelines =
        prepareGroupedLogicalSourcePipelines(triplesMapFilter);
    Set<Set<Map<? extends TriplesMapper<?, T>, Flux<T>>>> intermediaryResults = new HashSet<>();

    for (Map.Entry<Object, List<LogicalSourcePipeline<?, T>>> groupedPipelineEntry : groupedLogicalSourcePipelines
        .entrySet()) {
      Object source = groupedPipelineEntry.getKey();
      List<LogicalSourcePipeline<?, T>> pipelineGroup = groupedPipelineEntry.getValue();
      var inCaseOfException = pipelineGroup.get(0)
          .getLogicalSource();

      Optional<Flux<DataBuffer>> resolvedSource = resolveSource(source, inCaseOfException, namedInputStreams);

      Set<Map<? extends TriplesMapper<?, T>, Flux<T>>> intermediaryPipelineResults = new HashSet<>();

      resolvedSource.ifPresentOrElse(
          dataBufferFlux -> processDataBufferFlux(dataBufferFlux, intermediaryPipelineResults, pipelineGroup,
              triplesMapFilter),
          () -> pipelineGroup.forEach(pipeline -> intermediaryPipelineResults.add(pipeline.run(triplesMapFilter))));

      intermediaryResults.add(intermediaryPipelineResults);
    }

    return finishPipelines(intermediaryResults);
  }

  private void processDataBufferFlux(Flux<DataBuffer> dataBufferFlux,
      Set<Map<? extends TriplesMapper<?, T>, Flux<T>>> intermediaryPipelineResults,
      List<LogicalSourcePipeline<?, T>> pipelineGroup, Set<TriplesMap> triplesMapFilter) {
    // Defer publishing until all pipelines are connected
    Flux<DataBuffer> dataSource = dataBufferFlux.publish()
        .autoConnect(pipelineGroup.size());

    pipelineGroup.forEach(pipeline -> {
      try {
        intermediaryPipelineResults
            .add(pipeline.run(ReactiveInputStreams.inputStreamFrom(dataSource), triplesMapFilter));
      } catch (IOException ioException) {
        throw new RmlMapperException(
            String.format("Could not create input stream for logical source pipeline with logical source %s",
                exception(pipeline.getLogicalSource())));
      }
    });
  }

  public Flux<T> mapItem(Object item) {
    return mapItem(item, Set.of());
  }

  public Flux<T> mapItem(Object item, Set<TriplesMap> triplesMapFilter) {
    return Flux.merge(mapItemPerTriplesMap(item, triplesMapFilter).values());
  }

  public Map<TriplesMap, Flux<T>> mapItemPerTriplesMap(Object item, Set<TriplesMap> triplesMapFilter) {
    Map<Object, List<LogicalSourcePipeline<?, T>>> groupedLogicalSourcePipelines =
        prepareGroupedLogicalSourcePipelines(triplesMapFilter);
    Set<Set<Map<? extends TriplesMapper<?, T>, Flux<T>>>> intermediaryResults = new HashSet<>();

    for (Map.Entry<Object, List<LogicalSourcePipeline<?, T>>> groupedPipelineEntry : groupedLogicalSourcePipelines
        .entrySet()) {
      List<LogicalSourcePipeline<?, T>> pipelineGroup = groupedPipelineEntry.getValue();

      Set<Map<? extends TriplesMapper<?, T>, Flux<T>>> intermediaryPipelineResults = new HashSet<>();
      pipelineGroup.forEach(pipeline -> intermediaryPipelineResults.add(pipeline.run(item, triplesMapFilter)));
      intermediaryResults.add(intermediaryPipelineResults);
    }

    return finishPipelines(intermediaryResults);
  }

  private Map<Object, List<LogicalSourcePipeline<?, T>>> prepareGroupedLogicalSourcePipelines(
      Set<TriplesMap> triplesMapFilter) {
    Set<LogicalSourcePipeline<?, T>> logicalSourcePipelines = filterLogicalSourcePipeline(triplesMapFilter);

    return logicalSourcePipelines.stream()
        .collect(Collectors.groupingBy(logicalSourcePipeline -> logicalSourcePipeline.getLogicalSource()
            .getSource()));
  }

  private Map<TriplesMap, Flux<T>> finishPipelines(
      Set<Set<Map<? extends TriplesMapper<?, T>, Flux<T>>>> intermediaryResults) {
    Map<TriplesMapper<?, T>, Flux<T>> intermediaryFluxes = intermediaryResults.stream()
        .flatMap(Set::stream)
        .flatMap(map -> map.entrySet()
            .stream())
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<RefObjectMapper<T>, TriplesMapper<?, T>> roToParentTmMap =
        mapRefObjectMapperToParentTriplesMapper(intermediaryFluxes.keySet());

    Map<TriplesMapper<?, T>, Flux<T>> triplesMapperJoins = resolveJoins(roToParentTmMap, intermediaryFluxes);

    return intermediaryFluxes.entrySet()
        .stream()
        .collect(Collectors.toUnmodifiableMap(entry -> entry.getKey()
            .getTriplesMap(), entry -> Flux.merge(entry.getValue(), triplesMapperJoins.get(entry.getKey()))));
  }

  private Map<TriplesMapper<?, T>, Flux<T>> resolveJoins(Map<RefObjectMapper<T>, TriplesMapper<?, T>> roToParentTmMap,
      Map<TriplesMapper<?, T>, Flux<T>> intermediaryFluxes) {
    return intermediaryFluxes.entrySet()
        .stream()
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
            entry -> resolveTriplesMapperJoins(entry.getKey(), entry.getValue(), roToParentTmMap, intermediaryFluxes)));
  }

  private Flux<T> resolveTriplesMapperJoins(TriplesMapper<?, T> triplesMapper, Flux<T> tmFlux,
      Map<RefObjectMapper<T>, TriplesMapper<?, T>> roToParentTmMap,
      Map<TriplesMapper<?, T>, Flux<T>> intermediaryFluxes) {

    return Flux.merge(triplesMapper.getRefObjectMappers()
        .stream()
        .map(refObjectMapper -> refObjectMapper.resolveJoins(tmFlux, roToParentTmMap.get(refObjectMapper),
            intermediaryFluxes.get(roToParentTmMap.get(refObjectMapper))))
        .collect(Collectors.toUnmodifiableList()));
  }

  private Map<RefObjectMapper<T>, TriplesMapper<?, T>> mapRefObjectMapperToParentTriplesMapper(
      Set<TriplesMapper<?, T>> triplesMappers) {
    Map<RefObjectMapper<T>, TriplesMapper<?, T>> roToParentTm = new HashMap<>();
    refObjectMapperToParentTriplesMap.forEach((refObjectMapper, parentTriplesMap) -> triplesMappers.stream()
        .filter(triplesMapper -> triplesMapper.getTriplesMap()
            .equals(parentTriplesMap))
        .findFirst()
        .ifPresentOrElse(triplesMapper -> roToParentTm.put(refObjectMapper, triplesMapper), () -> {
          throw new TriplesMapperException(String.format(
              "Could not find corresponding triples map for parent triples map %s for %s%nPossibly the parent triples "
                  + "map does not exist, or the reference to it is misspelled?",
              exception(parentTriplesMap),
              exception(refObjectMapper.getTriplesMap(), refObjectMapper.getRefObjectMap())));
        }));

    return roToParentTm;
  }

  private Set<LogicalSourcePipeline<?, T>> filterLogicalSourcePipeline(Set<TriplesMap> triplesMapFilter) {
    if (triplesMapFilter == null || triplesMapFilter.isEmpty()) {
      return Set.copyOf(logicalSourcePipelinePool.values());
    }

    return triplesMapFilter.stream()
        .map(logicalSourcePipelinePool::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableSet());
  }

  private Optional<Flux<DataBuffer>> resolveSource(Object source, LogicalSource inCaseOfException,
      Map<String, InputStream> namedInputStreams) {
    if (source instanceof NameableStream) {
      NameableStream stream = (NameableStream) source;
      String unresolvedName = stream.getStreamName();
      String name = StringUtils.isBlank(unresolvedName) ? DEFAULT_STREAM_NAME : unresolvedName;

      if (!namedInputStreams.containsKey(name)) {
        throw new RmlMapperException(String.format("Could not resolve input stream with name %s for logical source %s",
            stream.getStreamName(), exception(inCaseOfException)));
      }

      return Optional.of(ReactiveInputStreams.fluxInputStream(namedInputStreams.get(name)));
    }

    return sourceResolver.apply(source);
  }
}
