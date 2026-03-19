package io.carml.engine;

import static io.carml.util.LogUtil.exception;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolver;
import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.SourceRecordCache;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.NameableStream;
import io.carml.model.Source;
import io.carml.model.TriplesMap;
import io.carml.output.NTriplesTermEncoder;
import io.carml.util.Mappings;
import io.carml.util.TypeRef;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public abstract class RmlMapper<T, K> {
    public static final String DEFAULT_STREAM_NAME = "DEFAULT";

    @Getter
    private final Set<TriplesMap> triplesMaps;

    private final Set<SourceResolver<?>> sourceResolvers;

    private final MappingPipeline<T> mappingPipeline;

    private final Map<K, Set<MergeableMappingResult<K, T>>> mergeables;

    /**
     * Resolved mappings for the mappable subset of triples maps. Each entry pairs an original
     * TriplesMap with its effective LogicalView (explicit or synthetic). Computed eagerly at build
     * time via {@link MappingResolver#resolve(Set)}.
     */
    @Getter
    private final List<ResolvedMapping> resolvedMappings;

    /**
     * LV-based triples mappers, keyed by TriplesMap. Empty when no TriplesMap uses an explicit
     * LogicalView.
     */
    private final Map<TriplesMap, TriplesMapper<T>> lvTriplesMappers;

    /**
     * Evaluator for LogicalView-based TriplesMap instances. Null when no LV TMs exist.
     */
    private final LogicalViewEvaluator logicalViewEvaluator;

    protected RmlMapper(
            @NonNull Set<TriplesMap> triplesMaps,
            @NonNull MappingPipeline<T> mappingPipeline,
            @NonNull Set<SourceResolver<?>> sourceResolvers,
            @NonNull List<ResolvedMapping> resolvedMappings,
            @NonNull Map<TriplesMap, TriplesMapper<T>> lvTriplesMappers,
            LogicalViewEvaluator logicalViewEvaluator) {
        this.triplesMaps = triplesMaps;
        this.sourceResolvers = sourceResolvers;
        this.mappingPipeline = mappingPipeline;
        this.mergeables = new HashMap<>();
        this.resolvedMappings = resolvedMappings;
        this.lvTriplesMappers = lvTriplesMappers;
        this.logicalViewEvaluator = logicalViewEvaluator;
    }

    public <R> Flux<T> mapRecord(R providedRecord, Class<R> providedRecordClass) {
        return mapRecord(providedRecord, providedRecordClass, Set.of());
    }

    public <R> Flux<T> mapRecord(R providedRecord, Class<R> providedRecordClass, Set<TriplesMap> triplesMapFilter) {
        var typeRef = TypeRef.forClass(providedRecordClass);
        return map(null, providedRecord, typeRef, triplesMapFilter).flatMap(MappingResult::getResults);
    }

    public <R> Flux<T> mapRecord(R providedRecord, TypeRef<R> providedRecordTypeRef) {
        return mapRecord(providedRecord, providedRecordTypeRef, Set.of());
    }

    public <R> Flux<T> mapRecord(R providedRecord, TypeRef<R> providedRecordTypeRef, Set<TriplesMap> triplesMapFilter) {
        return map(null, providedRecord, providedRecordTypeRef, triplesMapFilter)
                .flatMap(MappingResult::getResults);
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
            TypeRef<V> providedRecordTypeRef,
            Set<TriplesMap> triplesMapFilter) {

        if (providedRecord != null) {
            // mapRecord() path — uses LS pipeline
            var mappingContext = MappingContext.<T>builder()
                    .triplesMapFilter(triplesMapFilter)
                    .mappingPipeline(mappingPipeline)
                    .build();
            return Flux.fromIterable(
                            getSources(mappingContext, namedInputStreams, providedRecord, providedRecordTypeRef))
                    .flatMap(resolvedSourceEntry ->
                            mapSource(resolvedSourceEntry.getLeft(), mappingContext, resolvedSourceEntry.getRight()))
                    .mapNotNull(this::handleCompletable)
                    .concatWith(mergeMergeables())
                    .concatWith(checkStrictModeExpressions(mappingContext));
        }

        // Normal map() path — uses view pipeline exclusively
        return mapLogicalViews(namedInputStreams, triplesMapFilter)
                .mapNotNull(this::handleCompletable)
                .concatWith(mergeMergeables());
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

    private <V> Set<Pair<Source, ResolvedSource<?>>> getSources(
            MappingContext<T> mappingContext,
            Map<String, InputStream> namedInputStreams,
            V providedRecord,
            TypeRef<V> providedRecordTypeRef) {

        var logicalSourcesPerSource = mappingContext.getLogicalSourcesPerSource();

        if (providedRecord != null) {
            if (logicalSourcesPerSource.size() > 1) {
                throw new RmlMapperException(String.format(
                        "Multiple sources found when mapping provided record. This is not supported:%n%s",
                        exception(logicalSourcesPerSource.values().stream()
                                .flatMap(Set::stream)
                                .collect(toSet()))));
            }

            return Set.of(Pair.of(
                    logicalSourcesPerSource.keySet().iterator().next(),
                    ResolvedSource.of(providedRecord, providedRecordTypeRef)));
        }

        return logicalSourcesPerSource.entrySet().stream()
                .map(sourceEntry -> resolveSource(sourceEntry.getKey(), sourceEntry.getValue(), namedInputStreams))
                .collect(Collectors.toUnmodifiableSet());
    }

    private Pair<Source, ResolvedSource<?>> resolveSource(
            Source source, Set<LogicalSource> inCaseOfException, Map<String, InputStream> namedInputStreams) {
        if (source instanceof NameableStream stream) {
            String unresolvedName = stream.getStreamName();
            String name = StringUtils.isBlank(unresolvedName) ? DEFAULT_STREAM_NAME : unresolvedName;

            if (!namedInputStreams.containsKey(name)) {
                throw new RmlMapperException(String.format(
                        "Could not resolve input stream with name %s for logical source: %s",
                        name, exception(inCaseOfException.iterator().next())));
            }

            return Pair.of(source, ResolvedSource.of(Mono.just(namedInputStreams.get(name)), new TypeRef<>() {}));
        }

        var resolvedSource = sourceResolvers.stream()
                .filter(resolver -> resolver.supportsSource(source))
                .findFirst()
                .map(resolver -> resolver.apply(source)
                        .orElseThrow(() -> new RmlMapperException(String.format(
                                "Could not resolve source: %s",
                                exception(inCaseOfException.iterator().next())))))
                .orElseThrow(() -> new RmlMapperException(String.format(
                        "No source resolver found for source: %s",
                        exception(inCaseOfException.iterator().next()))));

        return Pair.of(source, resolvedSource);
    }

    @SuppressWarnings("unchecked")
    private Flux<MappingResult<T>> mapSource(
            Source source, MappingContext<T> mappingContext, ResolvedSource<?> resolvedSource) {
        return Flux.just(mappingPipeline.getSourceToLogicalSourceResolver().get(source))
                .flatMap(resolver -> ((LogicalSourceResolver<Object>) resolver)
                        .getLogicalSourceRecords(
                                mappingContext.logicalSourcesPerSource.get(source),
                                mappingPipeline.getExpressionsPerLogicalSource())
                        .apply(resolvedSource))
                .flatMap(logicalSourceRecord -> mapTriples(mappingContext, logicalSourceRecord));
    }

    private Flux<MappingResult<T>> mapTriples(
            MappingContext<T> mappingContext, LogicalSourceRecord<?> logicalSourceRecord) {
        return Flux.fromIterable(
                        mappingContext.getTriplesMappersForLogicalSource(logicalSourceRecord.getLogicalSource()))
                .flatMap(triplesMapper -> triplesMapper.map(logicalSourceRecord));
    }

    private Flux<MappingResult<T>> mapLogicalViews(
            Map<String, InputStream> namedInputStreams, Set<TriplesMap> triplesMapFilter) {
        var prepared = prepareLogicalViewPipeline(namedInputStreams, triplesMapFilter);
        if (prepared == null) {
            return Flux.empty();
        }

        return Flux.fromIterable(prepared.mappings()).flatMap(rm -> {
            var mapper = lvTriplesMappers.get(rm.getOriginalTriplesMap());
            return logicalViewEvaluator
                    .evaluate(
                            rm.getEffectiveView(),
                            prepared.sourceResolver(),
                            rm.getEvaluationContext(),
                            prepared.recordCache(),
                            prepared.logicalSourcesPerSource(),
                            prepared.expressionsPerLogicalSource())
                    .flatMap(mapper::map);
        });
    }

    /**
     * Maps logical views to N-Triples/N-Quads bytes using the statement-less byte pipeline. Regular
     * triples are encoded directly to bytes without creating Statement objects. Mergeable results
     * (from {@code rml:gather} with collections) are collected across iterations and encoded to bytes
     * after merging.
     *
     * @param namedInputStreams named input streams for resolving sources
     * @param triplesMapFilter filter for specific TriplesMap instances; empty for all
     * @param encoder the encoder to use for byte serialization
     * @param includeGraph whether to include the graph field in encoded output (true for N-Quads,
     *     false for N-Triples)
     * @return a Flux of encoded byte arrays
     */
    protected Flux<byte[]> mapLogicalViewsToBytes(
            Map<String, InputStream> namedInputStreams,
            Set<TriplesMap> triplesMapFilter,
            NTriplesTermEncoder encoder,
            boolean includeGraph) {
        var prepared = prepareLogicalViewPipeline(namedInputStreams, triplesMapFilter);
        if (prepared == null) {
            return Flux.empty();
        }

        return Flux.fromIterable(prepared.mappings())
                .flatMap(rm -> {
                    var mapper = lvTriplesMappers.get(rm.getOriginalTriplesMap());
                    return logicalViewEvaluator
                            .evaluate(
                                    rm.getEffectiveView(),
                                    prepared.sourceResolver(),
                                    rm.getEvaluationContext(),
                                    prepared.recordCache(),
                                    prepared.logicalSourcesPerSource(),
                                    prepared.expressionsPerLogicalSource())
                            .flatMapIterable(iteration -> {
                                var result = mapper.mapToBytes(iteration, encoder, includeGraph);
                                for (var mergeable : result.mergeables()) {
                                    handleCompletable(mergeable);
                                }
                                return result.bytes();
                            });
                })
                .concatWith(encodeMergeables(encoder, includeGraph));
    }

    /**
     * Prepares the shared pipeline components for logical view mapping: filters the resolved
     * mappings, pre-collects LogicalSource information across all views, and creates a source record
     * cache. Returns {@code null} when no mappable views exist (caller should return
     * {@code Flux.empty()}).
     */
    private PreparedPipeline prepareLogicalViewPipeline(
            Map<String, InputStream> namedInputStreams, Set<TriplesMap> triplesMapFilter) {
        if (logicalViewEvaluator == null || lvTriplesMappers.isEmpty()) {
            return null;
        }

        var filteredMappings = resolvedMappings.stream()
                .filter(rm -> triplesMapFilter.isEmpty() || triplesMapFilter.contains(rm.getOriginalTriplesMap()))
                .filter(rm -> lvTriplesMappers.containsKey(rm.getOriginalTriplesMap()))
                .toList();

        if (filteredMappings.isEmpty()) {
            return null;
        }

        // Pre-collect LogicalSource information across all views (including join parents
        // recursively). This allows the resolver to be called once per Source with the
        // full set of LogicalSources, and the resulting records are cached and filtered
        // per view.
        var logicalSourcesPerSource = new LinkedHashMap<Source, Set<LogicalSource>>();
        var expressionsPerLogicalSource = new LinkedHashMap<LogicalSource, Set<String>>();
        var allViews = filteredMappings.stream()
                .map(ResolvedMapping::getEffectiveView)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        SourceRecordCache.collectLogicalSourceInfo(allViews, logicalSourcesPerSource, expressionsPerLogicalSource);

        // Use noop cache for a single view without joins to preserve lazy streaming.
        // Otherwise, use a real cache to avoid re-parsing shared sources.
        var needsCaching = filteredMappings.size() > 1
                || filteredMappings.stream().anyMatch(rm -> hasJoins(rm.getEffectiveView()));
        var recordCache = needsCaching ? SourceRecordCache.create() : SourceRecordCache.noop();

        // Resolve sources eagerly — each Source is resolved once and shared via the cache.
        var resolvedSourceCache = new HashMap<Source, ResolvedSource<?>>();
        Function<Source, ResolvedSource<?>> cachingSourceResolver =
                source -> resolvedSourceCache.computeIfAbsent(source, s -> resolveSourceForView(s, namedInputStreams));

        return new PreparedPipeline(
                filteredMappings,
                cachingSourceResolver,
                recordCache,
                logicalSourcesPerSource,
                expressionsPerLogicalSource);
    }

    private record PreparedPipeline(
            List<ResolvedMapping> mappings,
            Function<Source, ResolvedSource<?>> sourceResolver,
            SourceRecordCache recordCache,
            Map<Source, Set<LogicalSource>> logicalSourcesPerSource,
            Map<LogicalSource, Set<String>> expressionsPerLogicalSource) {}

    /**
     * Encodes merged mergeable results to bytes. Called after all iterations have been processed
     * to handle cross-iteration collection merging (rml:gather).
     *
     * @param encoder the encoder to use for byte serialization
     * @param includeGraph whether to include the graph field in encoded output (true for N-Quads,
     *     false for N-Triples)
     */
    private Flux<byte[]> encodeMergeables(NTriplesTermEncoder encoder, boolean includeGraph) {
        return Flux.fromIterable(mergeables.values())
                .flatMap(values -> values.stream()
                        .reduce(MergeableMappingResult::merge)
                        .map(merged -> Flux.from(merged.getResults()).map(result -> {
                            var stmt = (org.eclipse.rdf4j.model.Statement) result;
                            if (includeGraph) {
                                return encoder.encodeNQuad(
                                        stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
                            }
                            return encoder.encodeNTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
                        }))
                        .orElse(Flux.empty()));
    }

    private ResolvedSource<?> resolveSourceForView(Source source, Map<String, InputStream> namedInputStreams) {
        if (source instanceof NameableStream stream) {
            var name = StringUtils.isBlank(stream.getStreamName()) ? DEFAULT_STREAM_NAME : stream.getStreamName();
            if (namedInputStreams != null && namedInputStreams.containsKey(name)) {
                return ResolvedSource.of(Mono.just(namedInputStreams.get(name)), new TypeRef<>() {});
            }
            throw new RmlMapperException(
                    "Could not resolve input stream with name %s for logical view source".formatted(name));
        }

        return sourceResolvers.stream()
                .filter(resolver -> resolver.supportsSource(source))
                .findFirst()
                .flatMap(resolver -> resolver.apply(source))
                .orElseThrow(() -> new RmlMapperException("No source resolver found for logical view source"));
    }

    private static boolean hasJoins(LogicalView view) {
        return (view.getLeftJoins() != null && !view.getLeftJoins().isEmpty())
                || (view.getInnerJoins() != null && !view.getInnerJoins().isEmpty());
    }

    /**
     * Runs strict-mode expression checks on all LS-pipeline TriplesMappers. Only relevant for the
     * {@code mapRecord()} path; the view pipeline validates field existence eagerly during
     * evaluation.
     */
    private Flux<MappingResult<T>> checkStrictModeExpressions(MappingContext<T> mappingContext) {
        Flux<TriplesMapper<T>> mappers = Flux.fromIterable(
                        mappingContext.getTriplesMapperPerLogicalSource().values())
                .flatMapIterable(triplesMappers -> triplesMappers);
        return mappers.concatMap(
                triplesMapper -> triplesMapper.checkStrictModeExpressions().then(Mono.empty()));
    }

    private Flux<MappingResult<T>> mergeMergeables() {
        return Flux.fromIterable(mergeables.values())
                .flatMap(values -> values.stream()
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

                return new MappingContext<>(
                        actionableTriplesMaps, filteredTriplesMappersPerLogicalSource, filteredLogicalSourcesPerSource);
            }
        }
    }
}
