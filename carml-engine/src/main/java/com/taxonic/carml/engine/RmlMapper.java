package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.NameableStream;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.LogUtil;
import com.taxonic.carml.util.ReactorUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor
public class RmlMapper<T> {

  private static final String DEFAULT_STREAM_NAME = "DEFAULT";

  private final Function<Object, Optional<Flux<DataBuffer>>> sourceResolver;

  private final Map<TriplesMap, LogicalSourcePipeline<?, T>> logicalSourcePipelinePool;

  public Flux<T> map() {
    return map(Map.of());
  }

  public Flux<T> map(Set<TriplesMap> triplesMapFilter) {
    return map(Map.of(), triplesMapFilter);
  }

  public Flux<T> map(@NonNull InputStream inputStream) {
    return map(ImmutableMap.of(DEFAULT_STREAM_NAME, inputStream));
  }

  public Flux<T> map(@NonNull InputStream inputStream, Set<TriplesMap> triplesMapFilter) {
    return map(ImmutableMap.of(DEFAULT_STREAM_NAME, inputStream), triplesMapFilter);
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
    return mapPerTriplesMap(ImmutableMap.of(DEFAULT_STREAM_NAME, inputStream));
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap(@NonNull InputStream inputStream, Set<TriplesMap> triplesMapFilter) {
    return mapPerTriplesMap(ImmutableMap.of(DEFAULT_STREAM_NAME, inputStream), triplesMapFilter);
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap(Map<String, InputStream> namedInputStreams) {
    return mapPerTriplesMap(namedInputStreams, Set.of());
  }

  public Map<TriplesMap, Flux<T>> mapPerTriplesMap(Map<String, InputStream> namedInputStreams,
      Set<TriplesMap> triplesMapFilter) {
    Set<LogicalSourcePipeline<?, T>> logicalSourcePipelines = filterLogicalSourcePipeline(triplesMapFilter);

    Map<Object, List<LogicalSourcePipeline<?, T>>> groupedLogicalSourcePipelines = logicalSourcePipelines.stream()
        .collect(Collectors.groupingBy(logicalSourcePipeline -> logicalSourcePipeline.getLogicalSource()
            .getSource()));

    Set<Set<Map<TriplesMap, Flux<T>>>> overallResults = new HashSet<>();

    for (Map.Entry<Object, List<LogicalSourcePipeline<?, T>>> groupedPipelineEntry : groupedLogicalSourcePipelines
        .entrySet()) {
      Object source = groupedPipelineEntry.getKey();
      List<LogicalSourcePipeline<?, T>> pipelineGroup = groupedPipelineEntry.getValue();
      LogicalSource inCaseOfException = pipelineGroup.get(0)
          .getLogicalSource();

      Optional<Flux<DataBuffer>> resolvedSource = resolveSource(source, inCaseOfException, namedInputStreams);

      Set<Map<TriplesMap, Flux<T>>> pipelineResults = new HashSet<>();

      resolvedSource.ifPresentOrElse(dataBufferFlux -> {
        // Defer publishing until all pipelines are connected
        Flux<DataBuffer> dataSource = dataBufferFlux.publish()
            .autoConnect(pipelineGroup.size());

        pipelineGroup.forEach(pipeline -> {
          try {
            pipelineResults.add(pipeline.run(ReactorUtil.inputStreamFrom(dataSource)));
          } catch (IOException ioException) {
            throw new RmlMapperException(
                String.format("Could not create input stream for logical source pipeline with logical source %s",
                    LogUtil.exception(pipeline.getLogicalSource())));
          }
        });
      }, () -> pipelineGroup.forEach(pipeline -> pipelineResults.add(pipeline.run())));

      overallResults.add(pipelineResults);
    }

    return overallResults.stream()
        .flatMap(Set::stream)
        .flatMap(map -> map.entrySet()
            .stream())
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Set<LogicalSourcePipeline<?, T>> filterLogicalSourcePipeline(Set<TriplesMap> triplesMapFilter) {
    if (triplesMapFilter == null || triplesMapFilter.isEmpty()) {
      return ImmutableSet.copyOf(logicalSourcePipelinePool.values());
    }

    return triplesMapFilter.stream()
        .map(logicalSourcePipelinePool::get)
        .filter(Objects::nonNull)
        .collect(ImmutableSet.toImmutableSet());
  }

  private Optional<Flux<DataBuffer>> resolveSource(Object source, LogicalSource inCaseOfException,
      Map<String, InputStream> namedInputStreams) {
    if (source instanceof NameableStream) {
      NameableStream stream = (NameableStream) source;
      String unresolvedName = stream.getStreamName();
      String name = StringUtils.isBlank(unresolvedName) ? DEFAULT_STREAM_NAME : unresolvedName;

      if (!namedInputStreams.containsKey(name)) {
        throw new RmlMapperException(String.format("Could not resolve input stream with name %s for logical source %s",
            stream.getStreamName(), LogUtil.exception(inCaseOfException)));
      }

      return Optional.of(ReactorUtil.fluxInputStream(namedInputStreams.get(name)));
    }

    return sourceResolver.apply(source);
  }
}
