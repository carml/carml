package com.taxonic.carml.engine;

import static com.taxonic.carml.util.LogUtil.exception;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.Mapping;
import com.taxonic.carml.util.ReactiveInputStreams;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor
public abstract class RmlMapper<T> {

  public static final String DEFAULT_STREAM_NAME = "DEFAULT";

  @Getter
  private Set<TriplesMap> triplesMaps;

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

  public Flux<T> mapItem(Object item) {
    return mapItem(item, Set.of());
  }

  public Flux<T> mapItem(Object item, Set<TriplesMap> triplesMapFilter) {
    return Flux.merge(mapPerTriplesMap(null, item, triplesMapFilter).values());
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

  private Map<TriplesMap, Flux<T>> mapPerTriplesMap(Map<String, InputStream> namedInputStreams,
      Set<TriplesMap> triplesMapFilter) {
    return mapPerTriplesMap(namedInputStreams, null, triplesMapFilter);
  }

  private Map<TriplesMap, Flux<T>> mapPerTriplesMap(Map<String, InputStream> namedInputStreams, Object providedItem,
      Set<TriplesMap> triplesMapFilter) {

    var actionableTriplesMapFilter = Mapping.filterMappable(triplesMapFilter);

    var intermediaryResults = logicalSourcePipelinePool.entrySet()
        .stream()
        .filter(pipelineEntry -> filterMappablePipelines(pipelineEntry, triplesMapFilter))
        .collect(groupingBy(this::getPipelineSourceObject))
        .entrySet()
        .stream()
        .map(pipelineGroups -> resolveSource(pipelineGroups, namedInputStreams, providedItem))
        .map(this::flattenPipelineGroup)
        .map(pipelineGroups -> runPipeline(pipelineGroups, actionableTriplesMapFilter))
        .collect(toUnmodifiableSet());

    return finishPipelines(intermediaryResults, actionableTriplesMapFilter);
  }

  private boolean filterMappablePipelines(Map.Entry<TriplesMap, LogicalSourcePipeline<?, T>> pipelineEntry,
      Set<TriplesMap> triplesMapFilter) {
    return triplesMapFilter == null || triplesMapFilter.isEmpty() || triplesMapFilter.contains(pipelineEntry.getKey());
  }

  private Object getPipelineSourceObject(Map.Entry<TriplesMap, LogicalSourcePipeline<?, T>> pipelineEntry) {
    return pipelineEntry.getValue()
        .getLogicalSource()
        .getSource();
  }

  private Map.Entry<Optional<Object>, List<Map.Entry<TriplesMap, LogicalSourcePipeline<?, T>>>> resolveSource(
      Map.Entry<Object, List<Map.Entry<TriplesMap, LogicalSourcePipeline<?, T>>>> groupedPipelines,
      Map<String, InputStream> namedInputStreams, Object providedItem) {
    Optional<Object> resolvedSource;
    if (providedItem != null) {
      resolvedSource = Optional.of(providedItem);
    } else {
      var source = groupedPipelines.getKey();
      var logicalSourceInCaseOfException = groupedPipelines.getValue()
          .get(0)
          .getKey()
          .getLogicalSource();

      resolvedSource = Optional
          .ofNullable(resolveInputStreamSource(source, logicalSourceInCaseOfException, namedInputStreams).orElse(null));
    }
    return Map.entry(resolvedSource, groupedPipelines.getValue());
  }

  private Optional<Flux<DataBuffer>> resolveInputStreamSource(Object source, LogicalSource inCaseOfException,
      Map<String, InputStream> namedInputStreams) {
    if (source instanceof NameableStream) {
      NameableStream stream = (NameableStream) source;
      String unresolvedName = stream.getStreamName();
      String name = StringUtils.isBlank(unresolvedName) ? DEFAULT_STREAM_NAME : unresolvedName;

      if (!namedInputStreams.containsKey(name)) {
        throw new RmlMapperException(String.format("Could not resolve input stream with name %s for logical source %s",
            name, exception(inCaseOfException)));
      }

      return Optional.of(ReactiveInputStreams.fluxInputStream(namedInputStreams.get(name)));
    }

    return sourceResolver.apply(source);
  }

  private Map.Entry<Optional<Object>, Set<LogicalSourcePipeline<?, T>>> flattenPipelineGroup(
      Map.Entry<Optional<Object>, List<Map.Entry<TriplesMap, LogicalSourcePipeline<?, T>>>> pipelineGroup) {
    var resolvedSource = pipelineGroup.getKey();
    Set<LogicalSourcePipeline<?, T>> pipelines = pipelineGroup.getValue()
        .stream()
        .map(Map.Entry::getValue)
        .collect(toUnmodifiableSet());

    return Map.entry(resolvedSource, pipelines);
  }

  private Set<Map<? extends TriplesMapper<?, T>, Flux<T>>> runPipeline(
      Map.Entry<Optional<Object>, Set<LogicalSourcePipeline<?, T>>> pipelineGroups,
      Set<TriplesMap> actionableTriplesMapFilter) {

    var optionalSourceObject = pipelineGroups.getKey();
    var pipelines = pipelineGroups.getValue();

    Set<Map<? extends TriplesMapper<?, T>, Flux<T>>> intermediaryResultSet = new HashSet<>();

    optionalSourceObject.ifPresentOrElse(
        sourceObject -> runPipeLinesForSourceObject(sourceObject, pipelines, actionableTriplesMapFilter,
            intermediaryResultSet),
        () -> pipelines.forEach(pipeline -> intermediaryResultSet.add(pipeline.run(actionableTriplesMapFilter))));

    return intermediaryResultSet;
  }

  @SuppressWarnings("unchecked")
  private void runPipeLinesForSourceObject(Object sourceObject, Set<LogicalSourcePipeline<?, T>> pipelines,
      Set<TriplesMap> actionableTriplesMapFilter,
      Set<Map<? extends TriplesMapper<?, T>, Flux<T>>> intermediaryResultSet) {
    if (sourceObject instanceof Flux<?>) {
      // Defer publishing until all pipelines are connected
      Flux<DataBuffer> dataSource = ((Flux<DataBuffer>) sourceObject).publish()
          .autoConnect(pipelines.size());

      pipelines.forEach(pipeline -> intermediaryResultSet
          .add(pipeline.run(getFluxInputStream(dataSource, pipelines), actionableTriplesMapFilter)));
    } else {
      pipelines.forEach(pipeline -> intermediaryResultSet.add(pipeline.run(sourceObject, actionableTriplesMapFilter)));
    }
  }

  private InputStream getFluxInputStream(Flux<DataBuffer> dataSource, Set<LogicalSourcePipeline<?, T>> pipelines) {
    try {
      return ReactiveInputStreams.inputStreamFrom(dataSource);
    } catch (IOException ioException) {
      throw new RmlMapperException(
          String.format("Could not create input stream for logical source pipeline with logical source %s",
              exception(pipelines.stream()
                  .findFirst()
                  .map(LogicalSourcePipeline::getLogicalSource)
                  .orElse(null))));
    }
  }

  private Map<TriplesMap, Flux<T>> finishPipelines(
      Set<Set<Map<? extends TriplesMapper<?, T>, Flux<T>>>> intermediaryResults,
      Set<TriplesMap> actionableTriplesMapFilter) {
    Map<TriplesMapper<?, T>, Flux<T>> intermediaryFluxes = intermediaryResults.stream()
        .flatMap(Set::stream)
        .flatMap(map -> map.entrySet()
            .stream())
        .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<RefObjectMapper<T>, TriplesMapper<?, T>> roToParentTmMap =
        mapRefObjectMapperToParentTriplesMapper(intermediaryFluxes.keySet(), actionableTriplesMapFilter);

    Map<TriplesMapper<?, T>, Flux<T>> triplesMapperJoins = resolveJoins(roToParentTmMap, intermediaryFluxes);

    return intermediaryFluxes.entrySet()
        .stream()
        .collect(toUnmodifiableMap(entry -> entry.getKey()
            .getTriplesMap(), entry -> Flux.merge(entry.getValue(), triplesMapperJoins.get(entry.getKey()))));
  }

  private Map<RefObjectMapper<T>, TriplesMapper<?, T>> mapRefObjectMapperToParentTriplesMapper(
      Set<TriplesMapper<?, T>> triplesMappers, Set<TriplesMap> actionableTriplesMapFilter) {
    Map<RefObjectMapper<T>, TriplesMapper<?, T>> roToParentTm = new HashMap<>();
    actionableRefObjectMapperToParentTriplesMap(actionableTriplesMapFilter)
        .forEach((refObjectMapper, parentTriplesMap) -> triplesMappers.stream()
            .filter(triplesMapper -> triplesMapper.getTriplesMap()
                .equals(parentTriplesMap))
            .findFirst()
            .ifPresentOrElse(triplesMapper -> roToParentTm.put(refObjectMapper, triplesMapper), () -> {
              throw new TriplesMapperException(String.format(
                  "Could not find corresponding triples map for parent triples map %s for %s%nPossibly the parent "
                      + "triples map does not exist, or the reference to it is misspelled?",
                  exception(parentTriplesMap),
                  exception(refObjectMapper.getTriplesMap(), refObjectMapper.getRefObjectMap())));
            }));

    return roToParentTm;
  }

  private Map<? extends RefObjectMapper<T>, TriplesMap> actionableRefObjectMapperToParentTriplesMap(
      Set<TriplesMap> actionableTriplesMapFilter) {
    if (actionableTriplesMapFilter == null || actionableTriplesMapFilter.isEmpty()) {
      return refObjectMapperToParentTriplesMap;
    }

    return refObjectMapperToParentTriplesMap.entrySet()
        .stream()
        .filter(entry -> actionableTriplesMapFilter.contains(entry.getKey()
            .getTriplesMap()))
        .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<TriplesMapper<?, T>, Flux<T>> resolveJoins(Map<RefObjectMapper<T>, TriplesMapper<?, T>> roToParentTmMap,
      Map<TriplesMapper<?, T>, Flux<T>> intermediaryFluxes) {
    return intermediaryFluxes.entrySet()
        .stream()
        .collect(toUnmodifiableMap(Map.Entry::getKey,
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
}
