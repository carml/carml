package io.carml.engine;

import static io.carml.util.LogUtil.exception;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

import com.google.common.collect.Iterables;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolver;
import io.carml.model.LogicalSource;
import io.carml.model.NameableStream;
import io.carml.model.Source;
import io.carml.model.TriplesMap;
import io.carml.util.Mappings;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor
public abstract class RmlMapper<T, K> {
    public static final String DEFAULT_STREAM_NAME = "DEFAULT";

    @Getter
    private final Set<TriplesMap> triplesMaps;

    private final SourceResolver sourceResolver;

    private final MappingPipeline<T> mappingPipeline;

    private final Map<K, Set<MergeableMappingResult<K, T>>> mergeables;

    public <R> Flux<T> mapRecord(R providedRecord, Class<R> providedRecordClass) {
        return mapRecord(providedRecord, providedRecordClass, Set.of());
    }

    public <R> Flux<T> mapRecord(R providedRecord, Class<R> providedRecordClass, Set<TriplesMap> triplesMapFilter) {
        return map(null, providedRecord, providedRecordClass, triplesMapFilter).flatMap(MappingResult::getResults);
    }

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
        return map(namedInputStreams, null, null, triplesMapFilter).flatMap(MappingResult::getResults);
    }

    private <V> Flux<MappingResult<T>> map(
            Map<String, InputStream> namedInputStreams,
            V providedRecord,
            Class<V> providedRecordClass,
            Set<TriplesMap> triplesMapFilter) {
        var mappingContext = MappingContext.<T>builder()
                .triplesMapFilter(triplesMapFilter)
                .mappingPipeline(mappingPipeline)
                .build();

        return Flux.fromIterable(getSources(mappingContext, namedInputStreams, providedRecord, providedRecordClass))
                .flatMap(resolvedSourceEntry -> mapSource(mappingContext, resolvedSourceEntry))
                .mapNotNull(this::handleCompletable)
                .filter(Objects::nonNull)
                .concatWith(resolveFinishers(mappingContext))
                .doOnTerminate(() -> mappingPipeline.getTriplesMappers().forEach(TriplesMapper::cleanup));
    }

    @SuppressWarnings("unchecked")
    private MappingResult<T> handleCompletable(MappingResult<T> mappingResult) {
        if (mappingResult instanceof MergeableMappingResult<?, ?> completable) {
            var completableToMerge = (MergeableMappingResult<K, T>) completable;
            var completableSet = new LinkedHashSet<MergeableMappingResult<K, T>>();
            completableSet.add(completableToMerge);
            mergeables.merge(completableToMerge.getKey(), completableSet, (c1, c2) -> {
                c1.addAll(c2);
                return c1;
            });
            return null;
        }

        return mappingResult;
    }

    private <V> Set<ResolvedSource<?>> getSources(
            MappingContext<T> mappingContext,
            Map<String, InputStream> namedInputStreams,
            V providedRecord,
            Class<V> providedRecordClass) {

        var logicalSourcesPerSource = mappingContext.getLogicalSourcesPerSource();

        if (providedRecord != null) {
            if (logicalSourcesPerSource.size() > 1) {
                throw new RmlMapperException(String.format(
                        "Multiple sources found when mapping provided record. This is not supported:%n%s",
                        exception(logicalSourcesPerSource.values().stream()
                                .flatMap(Set::stream)
                                .collect(toSet()))));
            }

            return Set.of(ResolvedSource.of(
                    Iterables.getFirst(logicalSourcesPerSource.keySet(), null), providedRecord, providedRecordClass));
        }

        return logicalSourcesPerSource.entrySet().stream()
                .map(sourceEntry -> resolveSource(sourceEntry.getKey(), sourceEntry.getValue(), namedInputStreams))
                .collect(Collectors.toUnmodifiableSet());
    }

    private ResolvedSource<?> resolveSource(
            Source source, Set<LogicalSource> inCaseOfException, Map<String, InputStream> namedInputStreams) {
        if (source instanceof NameableStream stream) {
            String unresolvedName = stream.getStreamName();
            String name = StringUtils.isBlank(unresolvedName) ? DEFAULT_STREAM_NAME : unresolvedName;

            if (!namedInputStreams.containsKey(name)) {
                throw new RmlMapperException(String.format(
                        "Could not resolve input stream with name %s for logical source: %s",
                        name, exception(Iterables.getFirst(inCaseOfException, null))));
            }

            return ResolvedSource.of(source, namedInputStreams.get(name), InputStream.class);
        }

        var resolved = sourceResolver
                .apply(source)
                .orElseThrow(() -> new RmlMapperException(String.format(
                        "Could not resolve source for logical source: %s",
                        exception(Iterables.getFirst(inCaseOfException, null)))));

        return ResolvedSource.of(source, resolved, Object.class);
    }

    private Flux<MappingResult<T>> mapSource(MappingContext<T> mappingContext, ResolvedSource<?> resolvedSource) {
        return Flux.just(mappingPipeline.getSourceToLogicalSourceResolver().get(resolvedSource.getRmlSource()))
                .flatMap(resolver -> resolver.getLogicalSourceRecords(
                                mappingContext.logicalSourcesPerSource.get(resolvedSource.getRmlSource()))
                        .apply(resolvedSource))
                .flatMap(logicalSourceRecord -> mapTriples(mappingContext, logicalSourceRecord));
    }

    private Flux<MappingResult<T>> mapTriples(
            MappingContext<T> mappingContext, LogicalSourceRecord<?> logicalSourceRecord) {
        return Flux.fromIterable(
                        mappingContext.getTriplesMappersForLogicalSource(logicalSourceRecord.getLogicalSource()))
                .flatMap(triplesMapper -> triplesMapper.map(logicalSourceRecord));
    }

    private Flux<MappingResult<T>> resolveFinishers(MappingContext<T> mappingContext) {
        return Flux.merge(resolveJoins(mappingContext), mergeMergeables());
    }

    private Flux<MappingResult<T>> resolveJoins(MappingContext<T> mappingContext) {
        return Flux.fromIterable(
                        mappingContext.getRefObjectMapperToParentTriplesMapper().entrySet())
                .flatMap(romMapperPtMapper -> romMapperPtMapper.getKey().resolveJoins(romMapperPtMapper.getValue()));
    }

    private Flux<MappingResult<T>> mergeMergeables() {
        return Flux.fromIterable(mergeables.values()).flatMap(values -> values.stream()
                .reduce(MergeableMappingResult::merge)
                .map(Flux::just)
                .orElse(Flux.empty()));
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class MappingContext<T> {
        private Set<TriplesMap> triplesMapFilter;

        private Map<LogicalSource, Set<TriplesMapper<T>>> triplesMapperPerLogicalSource;

        private Map<Source, Set<LogicalSource>> logicalSourcesPerSource;

        private Map<RefObjectMapper<T>, TriplesMapper<T>> refObjectMapperToParentTriplesMapper;

        public Set<TriplesMapper<T>> getTriplesMappersForLogicalSource(LogicalSource logicalSource) {
            return triplesMapperPerLogicalSource.get(logicalSource);
        }

        static <T> MappingContext.Builder<T> builder() {
            return new MappingContext.Builder<>();
        }

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        static class Builder<U> {
            private Set<TriplesMap> triplesMapFilter;

            private MappingPipeline<U> mappingPipeline;

            public MappingContext.Builder<U> triplesMapFilter(Set<TriplesMap> triplesMapFilter) {
                this.triplesMapFilter = triplesMapFilter;
                return this;
            }

            public MappingContext.Builder<U> mappingPipeline(MappingPipeline<U> mappingPipeline) {
                this.mappingPipeline = mappingPipeline;
                return this;
            }

            public MappingContext<U> build() {
                if (mappingPipeline == null) {
                    throw new RmlMapperException("Required mapping pipeline not provided.");
                }

                var triplesMappers = mappingPipeline.getTriplesMappers();

                var actionableTriplesMaps = Mappings.filterMappable(triplesMappers.stream()
                        .map(TriplesMapper::getTriplesMap)
                        .filter(triplesMap -> triplesMapFilter.isEmpty() || triplesMapFilter.contains(triplesMap))
                        .collect(toSet()));

                var filteredTriplesMappersPerLogicalSource = triplesMappers.stream()
                        .filter(triplesMapper -> actionableTriplesMaps.contains(triplesMapper.getTriplesMap()))
                        .collect(groupingBy(TriplesMapper::getLogicalSource, toSet()));

                var filteredLogicalSourcesPerSource = filteredTriplesMappersPerLogicalSource.keySet().stream()
                        .collect(groupingBy(LogicalSource::getSource, toSet()));

                var refObjectMapperToParentTriplesMapper = mappingPipeline.getRefObjectMapperToTriplesMapper();

                Map<RefObjectMapper<U>, TriplesMapper<U>> filteredRefObjectMapperToParentTriplesMapper =
                        refObjectMapperToParentTriplesMapper.entrySet().stream()
                                .filter(entry -> actionableTriplesMaps.contains(
                                        entry.getKey().getTriplesMap()))
                                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

                return new MappingContext<>(
                        actionableTriplesMaps,
                        filteredTriplesMappersPerLogicalSource,
                        filteredLogicalSourcesPerSource,
                        filteredRefObjectMapperToParentTriplesMapper);
            }
        }
    }
}
