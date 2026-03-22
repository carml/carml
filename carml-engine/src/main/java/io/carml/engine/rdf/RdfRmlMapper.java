package io.carml.engine.rdf;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.engine.CompositeObserver;
import io.carml.engine.MappedValue;
import io.carml.engine.MappingExecution;
import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.MappingPipeline;
import io.carml.engine.MappingResolver;
import io.carml.engine.ResolvedMapping;
import io.carml.engine.RmlMapper;
import io.carml.engine.RmlMapperException;
import io.carml.engine.TermGeneratorFactory;
import io.carml.engine.TriplesMapper;
import io.carml.engine.function.AnnotatedFunctionProvider;
import io.carml.engine.function.BuiltInFunctionProvider;
import io.carml.engine.function.FnoDescriptionProvider;
import io.carml.engine.function.FunctionDescriptor;
import io.carml.engine.function.FunctionProvider;
import io.carml.engine.function.FunctionRegistry;
import io.carml.engine.function.ParameterDescriptor;
import io.carml.engine.function.ReturnDescriptor;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.FileResolver;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolver;
import io.carml.logicalview.FileBasePathConfigurable;
import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.LogicalViewEvaluatorFactory;
import io.carml.model.Field;
import io.carml.model.IriSafeAnnotation;
import io.carml.model.LogicalSource;
import io.carml.model.Mapping;
import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import io.carml.output.NTriplesTermEncoder;
import io.carml.util.Mappings;
import io.carml.util.TypeRef;
import java.io.InputStream;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import reactor.core.publisher.Flux;

@Slf4j
public class RdfRmlMapper extends RmlMapper<Statement, MappedValue<Value>> {

    private static final IRI RML_BASE_IRI = iri("http://example.org/");

    private static final long SECONDS_TO_TIMEOUT = 30;

    @Getter
    private final MappingExecutionObserver observer;

    private RdfRmlMapper(
            Set<TriplesMap> triplesMaps,
            MappingPipeline<Statement> mappingPipeline,
            Set<SourceResolver<?>> sourceResolvers,
            List<ResolvedMapping> resolvedMappings,
            MappingExecutionObserver observer,
            Map<TriplesMap, TriplesMapper<Statement>> lvTriplesMappers,
            LogicalViewEvaluator logicalViewEvaluator) {
        super(
                triplesMaps,
                mappingPipeline,
                sourceResolvers,
                resolvedMappings,
                lvTriplesMappers,
                logicalViewEvaluator,
                observer);
        this.observer = observer;
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {

        private IRI baseIri = RML_BASE_IRI;

        private Mapping mapping;

        private Set<TriplesMap> providedTriplesMaps = new HashSet<>();

        private final List<Object> pendingFunctionObjects = new ArrayList<>();

        private final List<FunctionDescriptor> pendingDescriptors = new ArrayList<>();

        private final List<Model> pendingFnoDescriptions = new ArrayList<>();

        private final Set<SourceResolver<?>> sourceResolvers = new HashSet<>();

        private final FileResolver.FileResolverBuilder fileResolverBuilder = FileResolver.builder();

        private Supplier<ValueFactory> valueFactorySupplier = InterningValueFactory::new;

        private Normalizer.Form normalizationForm = Normalizer.Form.NFC;

        private boolean iriUpperCasePercentEncoding = true;

        private TermGeneratorFactory<Value> termGeneratorFactory;

        private final List<MappingExecutionObserver> observers = new ArrayList<>();

        private boolean strictMode = false;

        private boolean allowMultipleSubjectMaps = false;

        private Long limit;

        private Set<String> excludeLogicalSourceResolvers = new HashSet<>();

        private final List<LogicalViewEvaluatorFactory> logicalViewEvaluatorFactories = new ArrayList<>();

        /**
         * Sets the base IRI used in resolving relative IRIs produced by RML mappings.<br>
         * If not set, the base IRI will default to <code>"http://example.org/"</code> per the RML-Core spec.
         * Individual TriplesMap base IRIs (via <code>rml:baseIRI</code>) take precedence when present.
         *
         * @param baseIriString the base IRI String
         * @return {@link Builder}
         */
        public Builder baseIri(String baseIriString) {
            return baseIri(iri(baseIriString));
        }

        /**
         * Sets the base IRI used in resolving relative IRIs produced by RML mappings.<br>
         * If not set, the base IRI will default to <code>&lt;http://example.org/&gt;</code> per the RML-Core spec.
         * Individual TriplesMap base IRIs (via <code>rml:baseIRI</code>) take precedence when present.
         *
         * @param baseIri the base IRI
         * @return {@link Builder}
         */
        public Builder baseIri(IRI baseIri) {
            this.baseIri = baseIri;
            return this;
        }

        public Builder addFunctions(Object... fn) {
            pendingFunctionObjects.addAll(Arrays.asList(fn));
            return this;
        }

        /**
         * Loads function classes by fully-qualified class name, instantiates each via its default constructor,
         * and registers them as annotated function objects.
         *
         * @param classNames the fully-qualified class names of function classes
         * @return {@link Builder}
         */
        public Builder addFunctionClasses(String... classNames) {
            Object[] instances = Arrays.stream(classNames)
                    .map(name -> {
                        try {
                            return Class.forName(name).getDeclaredConstructor().newInstance();
                        } catch (ReflectiveOperationException e) {
                            throw new RmlMapperException("Failed to instantiate function class: " + name, e);
                        }
                    })
                    .toArray();
            return addFunctions(instances);
        }

        /**
         * Registers function descriptions from an RDF model containing FnO (Function Ontology)
         * descriptions and Java implementation bindings.
         *
         * @param fnoModel the RDF model with FnO function descriptions and mappings
         * @return {@link Builder}
         */
        public Builder addFunctionDescriptions(Model fnoModel) {
            pendingFnoDescriptions.add(fnoModel);
            return this;
        }

        /**
         * Parses the given input stream as RDF in the specified format and registers the resulting
         * FnO function descriptions.
         *
         * @param inputStream the input stream containing RDF data
         * @param format the RDF serialization format
         * @return {@link Builder}
         */
        public Builder addFunctionDescriptions(InputStream inputStream, RDFFormat format) {
            try {
                var model = Rio.parse(inputStream, format);
                pendingFnoDescriptions.add(model);
            } catch (Exception exception) {
                throw new RmlMapperException("Failed to parse FnO description input", exception);
            }
            return this;
        }

        /**
         * Starts a fluent function builder for registering a lambda-based function.
         *
         * @param iri the function IRI
         * @return {@link FunctionBuilder}
         */
        public FunctionBuilder function(String iri) {
            return new FunctionBuilder(this, iri);
        }

        public Builder sourceResolver(SourceResolver<?> sourceResolver) {
            sourceResolvers.add(sourceResolver);
            return this;
        }

        public Builder fileResolver(Path basePath) {
            fileResolverBuilder.basePath(basePath);
            return this;
        }

        public Builder classPathResolver(String classPathBasePath) {
            fileResolverBuilder.classPathBase(classPathBasePath);
            return this;
        }

        public Builder classPathResolver(ClassPathResolver classPathResolver) {
            fileResolverBuilder.classPathBase(classPathResolver.getClassPathBase());
            fileResolverBuilder.loadingClass(classPathResolver.getLoadingClass());
            return this;
        }

        public Builder valueFactorySupplier(Supplier<ValueFactory> valueFactorySupplier) {
            this.valueFactorySupplier = valueFactorySupplier;
            return this;
        }

        public Builder iriUnicodeNormalization(Normalizer.Form normalizationForm) {
            this.normalizationForm = normalizationForm;
            return this;
        }

        /**
         * Builder option for backwards compatibility. RmlMapper used to percent encode IRIs with lower case
         * hex numbers. Now, the default is upper case hex numbers.
         *
         * @param iriUpperCasePercentEncoding true for upper case, false for lower case
         * @return {@link Builder}
         */
        public Builder iriUpperCasePercentEncoding(boolean iriUpperCasePercentEncoding) {
            this.iriUpperCasePercentEncoding = iriUpperCasePercentEncoding;
            return this;
        }

        public Builder mapping(Mapping mapping) {
            this.mapping = mapping;
            fileResolverBuilder.mapping(mapping);
            return this;
        }

        public Builder triplesMaps(Set<TriplesMap> triplesMaps) {
            this.providedTriplesMaps = triplesMaps;
            return this;
        }

        /**
         * Enables or disables strict mode. When strict mode is enabled, the mapper will raise a data
         * error if a reference expression never produces a non-null result across all records of a logical
         * source. In lenient mode (default), non-matching references are silently skipped.
         *
         * @param strictMode true to enable strict mode, false for lenient mode
         * @return {@link Builder}
         */
        public Builder strictMode(boolean strictMode) {
            this.strictMode = strictMode;
            return this;
        }

        /**
         * Enables or disables support for multiple subject maps per TriplesMap. The RML spec mandates
         * exactly one {@code rml:subjectMap} per TriplesMap. When disabled (the default), the mapper
         * throws an error if a TriplesMap has more than one subject map. When enabled, the mapper
         * processes all subject maps and produces output for each.
         *
         * @param allowMultipleSubjectMaps true to allow multiple subject maps, false to reject them
         * @return {@link Builder}
         */
        public Builder allowMultipleSubjectMaps(boolean allowMultipleSubjectMaps) {
            this.allowMultipleSubjectMaps = allowMultipleSubjectMaps;
            return this;
        }

        /**
         * Sets the maximum number of iterations (records) to process per logical source. When set,
         * each mapping will produce at most this many iterations. By default, no limit is applied.
         *
         * @param limit the maximum number of iterations; must be positive
         * @return {@link Builder}
         * @throws IllegalArgumentException if limit is not positive
         */
        public Builder limit(long limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be positive, but was: %s".formatted(limit));
            }
            this.limit = limit;
            return this;
        }

        public Builder excludeLogicalSourceResolver(String resolverName) {
            excludeLogicalSourceResolvers.add(resolverName);
            return this;
        }

        public Builder excludeLogicalSourceResolver(Set<String> resolverNames) {
            excludeLogicalSourceResolvers = resolverNames;
            return this;
        }

        public Builder logicalViewEvaluatorFactory(LogicalViewEvaluatorFactory factory) {
            logicalViewEvaluatorFactories.add(factory);
            return this;
        }

        /**
         * Registers a {@link MappingExecutionObserver} that receives callbacks at key points in the
         * mapping pipeline. Multiple observers can be registered and are composed into a single
         * composite observer. When no observer is registered, a no-op implementation is used for
         * zero overhead.
         *
         * @param observer the observer to register
         * @return {@link Builder}
         */
        public Builder observer(MappingExecutionObserver observer) {
            observers.add(observer);
            return this;
        }

        public RdfRmlMapper build() {
            var resolverFactories = loadResolverFactories();

            var triplesMaps = mapping != null ? mapping.getTriplesMaps() : providedTriplesMaps;
            if (triplesMaps == null) {
                throw new RmlMapperException("No mappings provided.");
            }

            var rdfMapperConfig = buildRdfMapperConfig();

            var mappableTriplesMaps = Mappings.filterMappable(triplesMaps);

            var resolvedMappings = MappingResolver.resolve(mappableTriplesMaps, limit);
            var observer = CompositeObserver.of(observers);

            var lsTriplesMaps = mappableTriplesMaps.stream()
                    .filter(tm -> tm.getLogicalSource() instanceof LogicalSource)
                    .filter(tm -> hasMatchingResolver((LogicalSource) tm.getLogicalSource(), resolverFactories))
                    .collect(toUnmodifiableSet());

            if (mappableTriplesMaps.isEmpty()) {
                throw new RmlMapperException("No executable triples maps found.");
            }

            // LS pipeline: built for LS-based TMs (for mapRecord() API support)
            var mappingPipeline =
                    buildLsPipeline(lsTriplesMaps, rdfMapperConfig, resolverFactories, resolvedMappings, observer);

            // LV mappers: built for ALL TMs (both LS-based via implicit views AND LV-based)
            var lvResult =
                    buildLvMappers(mappableTriplesMaps, rdfMapperConfig, resolverFactories, resolvedMappings, observer);
            registerSourceResolvers();

            return new RdfRmlMapper(
                    triplesMaps,
                    mappingPipeline,
                    sourceResolvers,
                    resolvedMappings,
                    observer,
                    lvResult.mappers(),
                    lvResult.evaluator());
        }

        private record LvPipelineResult(
                Map<TriplesMap, TriplesMapper<Statement>> mappers, LogicalViewEvaluator evaluator) {}

        private static boolean hasMatchingResolver(
                LogicalSource logicalSource, Set<MatchingLogicalSourceResolverFactory> resolverFactories) {
            return resolverFactories.stream()
                    .anyMatch(factory -> factory.apply(logicalSource).isPresent());
        }

        private Set<MatchingLogicalSourceResolverFactory> loadResolverFactories() {
            var factories = ServiceLoader.load(MatchingLogicalSourceResolverFactory.class).stream()
                    .map(ServiceLoader.Provider::get)
                    .filter(not(factory -> excludeLogicalSourceResolvers.contains(factory.getResolverName())))
                    .collect(toUnmodifiableSet());

            if (factories.isEmpty()) {
                throw new RmlMapperException("No logical source resolver suppliers specified.");
            }
            return factories;
        }

        private RdfMapperConfig buildRdfMapperConfig() {
            var functionRegistry = buildFunctionRegistry();

            var rdfTermGeneratorConfig = RdfTermGeneratorConfig.builder()
                    .baseIri(baseIri)
                    .valueFactory(valueFactorySupplier.get())
                    .normalizationForm(normalizationForm)
                    .iriUpperCasePercentEncoding(iriUpperCasePercentEncoding)
                    .functionRegistry(functionRegistry)
                    .build();

            return RdfMapperConfig.builder()
                    .valueFactorySupplier(valueFactorySupplier)
                    .termGeneratorFactory(termGeneratorFactory)
                    .rdfTermGeneratorConfig(rdfTermGeneratorConfig)
                    .strictMode(strictMode)
                    .allowMultipleSubjectMaps(allowMultipleSubjectMaps)
                    .build();
        }

        private MappingPipeline<Statement> buildLsPipeline(
                Set<TriplesMap> lsTriplesMaps,
                RdfMapperConfig rdfMapperConfig,
                Set<MatchingLogicalSourceResolverFactory> resolverFactories,
                List<ResolvedMapping> resolvedMappings,
                MappingExecutionObserver observer) {
            var mappingPipeline = lsTriplesMaps.isEmpty()
                    ? MappingPipeline.<Statement>of(Set.of(), Map.of(), Map.of())
                    : RdfMappingPipelineFactory.getInstance()
                            .getMappingPipeline(lsTriplesMaps, rdfMapperConfig, resolverFactories);

            wireMappers(mappingPipeline.getTriplesMappers(), resolvedMappings, observer);
            return mappingPipeline;
        }

        private LvPipelineResult buildLvMappers(
                Set<TriplesMap> allTriplesMaps,
                RdfMapperConfig rdfMapperConfig,
                Set<MatchingLogicalSourceResolverFactory> resolverFactories,
                List<ResolvedMapping> resolvedMappings,
                MappingExecutionObserver observer) {

            var evaluatorFactories = logicalViewEvaluatorFactories.isEmpty()
                    ? ServiceLoader.load(LogicalViewEvaluatorFactory.class).stream()
                            .map(ServiceLoader.Provider::get)
                            .toList()
                    : List.copyOf(logicalViewEvaluatorFactories);

            configureFileBasePath(evaluatorFactories);

            var evaluator = new FactoryDelegatingEvaluator(evaluatorFactories);

            // Build an index for O(1) lookup of ResolvedMapping by TriplesMap
            var resolvedMappingIndex = new HashMap<TriplesMap, ResolvedMapping>();
            for (var rm : resolvedMappings) {
                resolvedMappingIndex.putIfAbsent(rm.getOriginalTriplesMap(), rm);
            }

            Map<TriplesMap, TriplesMapper<Statement>> lvMappers = new HashMap<>();
            for (var tm : allTriplesMaps) {
                var effectiveConfig = getEffectiveMapperConfig(tm, rdfMapperConfig);
                var rm = resolvedMappingIndex.get(tm);

                var iriSafeFieldNames = extractIriSafeFieldNames(rm);

                if (!iriSafeFieldNames.isEmpty()) {
                    var overriddenTermGenConfig = effectiveConfig.getRdfTermGeneratorConfig().toBuilder()
                            .iriSafeFieldNames(iriSafeFieldNames)
                            .build();
                    effectiveConfig = effectiveConfig.toBuilder()
                            .rdfTermGeneratorConfig(overriddenTermGenConfig)
                            .build();
                }

                Map<RefObjectMap, String> prefixes = rm != null ? rm.getRefObjectMapPrefixes() : Map.of();

                lvMappers.put(tm, RdfTriplesMapper.ofForView(tm, effectiveConfig, prefixes));
            }

            wireMappers(lvMappers.values(), resolvedMappings, observer);
            return new LvPipelineResult(Map.copyOf(lvMappers), evaluator);
        }

        private void wireMappers(
                Iterable<? extends TriplesMapper<Statement>> mappers,
                List<ResolvedMapping> resolvedMappings,
                MappingExecutionObserver observer) {
            for (var resolvedMapping : resolvedMappings) {
                for (var mapper : mappers) {
                    if (mapper instanceof RdfTriplesMapper<?> rtm
                            && rtm.getTriplesMap().equals(resolvedMapping.getOriginalTriplesMap())) {
                        rtm.setResolvedMapping(resolvedMapping);
                        rtm.setObserver(observer);
                        break;
                    }
                }
            }
        }

        private void configureFileBasePath(List<LogicalViewEvaluatorFactory> factories) {
            fileResolverBuilder.resolveFileBasePath().ifPresent(path -> {
                for (var factory : factories) {
                    if (factory instanceof FileBasePathConfigurable configurable) {
                        configurable.setFileBasePath(path);
                    }
                }
            });
        }

        private void registerSourceResolvers() {
            sourceResolvers.add(fileResolverBuilder.build());
            ServiceLoader.load(SourceResolver.class).stream()
                    .<SourceResolver<?>>map(ServiceLoader.Provider::get)
                    .forEach(sourceResolvers::add);
        }

        private static Set<String> extractIriSafeFieldNames(ResolvedMapping rm) {
            if (rm == null) {
                return Set.of();
            }
            var annotations = rm.getEffectiveView().getStructuralAnnotations();
            if (annotations == null) {
                return Set.of();
            }
            return annotations.stream()
                    .filter(IriSafeAnnotation.class::isInstance)
                    .flatMap(a -> {
                        var fields = a.getOnFields();
                        return fields == null ? Stream.empty() : fields.stream();
                    })
                    .map(Field::getFieldName)
                    .collect(toUnmodifiableSet());
        }

        private RdfMapperConfig getEffectiveMapperConfig(TriplesMap triplesMap, RdfMapperConfig rdfMapperConfig) {
            IRI triplesMapBaseIri = triplesMap.getBaseIri();
            if (triplesMapBaseIri == null) {
                return rdfMapperConfig;
            }

            var overriddenTermGenConfig = rdfMapperConfig.getRdfTermGeneratorConfig().toBuilder()
                    .baseIri(triplesMapBaseIri)
                    .build();

            return rdfMapperConfig.toBuilder()
                    .rdfTermGeneratorConfig(overriddenTermGenConfig)
                    .build();
        }

        private FunctionRegistry buildFunctionRegistry() {
            var registry = FunctionRegistry.create();

            // 1. Built-in functions (lowest priority)
            registry.registerAll(new BuiltInFunctionProvider());

            // 2. SPI-discovered FunctionProviders (override built-ins)
            ServiceLoader.load(FunctionProvider.class).stream()
                    .map(ServiceLoader.Provider::get)
                    .forEach(registry::registerAll);

            // 3a. FnO description-based registrations (override SPI)
            // Merge all FnO description models into a single model so that cross-file
            // references (e.g. function mappings referencing implementation classes defined
            // in a separate file) are resolved correctly.
            if (!pendingFnoDescriptions.isEmpty()) {
                var mergedModel =
                        pendingFnoDescriptions.stream().flatMap(Model::stream).collect(ModelCollector.toModel());

                registry.registerAll(new FnoDescriptionProvider(mergedModel));
            }

            // 3b. Programmatic registrations via addFunctions/addFunctionClasses (override 3a for same IRI)
            if (!pendingFunctionObjects.isEmpty()) {
                registry.registerAll(new AnnotatedFunctionProvider(pendingFunctionObjects.toArray()));
            }

            // 4. Lambda-based descriptors via function() chain (highest priority)
            pendingDescriptors.forEach(registry::register);

            return registry;
        }
    }

    /**
     * Fluent builder for registering lambda-based functions with the mapper.
     */
    public static class FunctionBuilder {

        private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

        private final Builder parent;

        private final String functionIri;

        private final List<ParameterDescriptor> params = new ArrayList<>();

        private ReturnDescriptor returnDesc;

        FunctionBuilder(Builder parent, String functionIri) {
            this.parent = parent;
            this.functionIri = functionIri;
        }

        /**
         * Adds a required parameter to this function.
         *
         * @param paramIri the parameter IRI
         * @param type the parameter Java type
         * @return this {@link FunctionBuilder}
         */
        public FunctionBuilder param(String paramIri, Class<?> type) {
            params.add(new ParameterDescriptor(VF.createIRI(paramIri), type, true));
            return this;
        }

        /**
         * Adds an optional parameter to this function.
         *
         * @param paramIri the parameter IRI
         * @param type the parameter Java type
         * @return this {@link FunctionBuilder}
         */
        public FunctionBuilder optionalParam(String paramIri, Class<?> type) {
            params.add(new ParameterDescriptor(VF.createIRI(paramIri), type, false));
            return this;
        }

        /**
         * Sets the return type of this function.
         *
         * @param type the return Java type
         * @return this {@link FunctionBuilder}
         */
        public FunctionBuilder returns(Class<?> type) {
            returnDesc = new ReturnDescriptor(null, type);
            return this;
        }

        /**
         * Completes the function registration with the given executor.
         *
         * @param executor the function implementation
         * @return the parent {@link Builder}
         */
        public Builder execute(Function<Map<IRI, Object>, Object> executor) {
            var iri = VF.createIRI(functionIri);
            var returns = returnDesc != null ? List.of(returnDesc) : List.of(new ReturnDescriptor(null, Object.class));
            var paramsCopy = List.copyOf(params);

            parent.pendingDescriptors.add(new FunctionDescriptor() {
                @Override
                public IRI getFunctionIri() {
                    return iri;
                }

                @Override
                public List<ParameterDescriptor> getParameters() {
                    return paramsCopy;
                }

                @Override
                public List<ReturnDescriptor> getReturns() {
                    return returns;
                }

                @Override
                public Object execute(Map<IRI, Object> parameterValues) {
                    return executor.apply(parameterValues);
                }
            });
            return parent;
        }
    }

    /**
     * Starts a mapping execution and returns a lifecycle handle for the resulting statement stream.
     * The handle provides access to the reactive statement flux, cancellation, checkpointing, and
     * runtime metrics.
     *
     * <p>For batch mode, this is equivalent to {@link #map()} wrapped in a lifecycle handle — the
     * flux completes when all source data is exhausted.
     *
     * @return a {@link MappingExecution} lifecycle handle
     */
    public MappingExecution start() {
        return MappingExecution.of(map(), observer, getResolvedMappings());
    }

    public Model mapToModel() {
        return toModel(map());
    }

    public Model mapToModel(Set<TriplesMap> triplesMapFilter) {
        return toModel(map(triplesMapFilter));
    }

    public Model mapToModel(@NonNull InputStream inputStream) {
        return toModel(map(inputStream));
    }

    public Model mapToModel(@NonNull InputStream inputStream, Set<TriplesMap> triplesMapFilter) {
        return toModel(map(inputStream, triplesMapFilter));
    }

    public Model mapToModel(Map<String, InputStream> namedInputStreams) {
        return toModel(map(namedInputStreams));
    }

    public Model mapToModel(Map<String, InputStream> namedInputStreams, Set<TriplesMap> triplesMapFilter) {
        return toModel(map(namedInputStreams, triplesMapFilter));
    }

    public <R> Model mapRecordToModel(R providedRecord, Class<R> providedRecordClass) {
        return toModel(mapRecord(providedRecord, providedRecordClass));
    }

    public <R> Model mapRecordToModel(
            R providedRecord, Class<R> providedRecordClass, Set<TriplesMap> triplesMapFilter) {
        return toModel(mapRecord(providedRecord, providedRecordClass, triplesMapFilter));
    }

    public <R> Model mapRecordToModel(R providedRecord, TypeRef<R> providedTypeRef) {
        return toModel(mapRecord(providedRecord, providedTypeRef));
    }

    public <R> Model mapRecordToModel(R providedRecord, TypeRef<R> providedTypeRef, Set<TriplesMap> triplesMapFilter) {
        return toModel(mapRecord(providedRecord, providedTypeRef, triplesMapFilter));
    }

    /**
     * Maps all logical views to N-Triples bytes using the statement-less byte pipeline. Each
     * element in the returned Flux is a single encoded N-Triples line ({@code subject predicate
     * object .\n}). Graph context is ignored (triples only).
     *
     * <p>This method bypasses {@link Statement} object creation entirely, encoding directly from
     * evaluated RDF terms to UTF-8 bytes for maximum throughput.
     *
     * @return a Flux of N-Triples encoded byte arrays
     */
    public Flux<byte[]> mapToNTriplesBytes() {
        return mapToNTriplesBytes(Map.of(), Set.of());
    }

    /**
     * Maps logical views to N-Triples bytes using the statement-less byte pipeline.
     *
     * @param namedInputStreams named input streams for resolving sources
     * @param triplesMapFilter filter for specific TriplesMap instances; empty for all
     * @return a Flux of N-Triples encoded byte arrays
     */
    public Flux<byte[]> mapToNTriplesBytes(
            Map<String, InputStream> namedInputStreams, Set<TriplesMap> triplesMapFilter) {
        return mapLogicalViewsToBytes(namedInputStreams, triplesMapFilter, NTriplesTermEncoder.withDefaults(), false);
    }

    /**
     * Maps all logical views to N-Quads bytes using the statement-less byte pipeline. Each element
     * in the returned Flux is a single encoded N-Quads line ({@code subject predicate object
     * [graph] .\n}). The graph field is included when present.
     *
     * <p>This method bypasses {@link Statement} object creation entirely, encoding directly from
     * evaluated RDF terms to UTF-8 bytes for maximum throughput.
     *
     * @return a Flux of N-Quads encoded byte arrays
     */
    public Flux<byte[]> mapToNQuadsBytes() {
        return mapToNQuadsBytes(Map.of(), Set.of());
    }

    /**
     * Maps logical views to N-Quads bytes using the statement-less byte pipeline.
     *
     * @param namedInputStreams named input streams for resolving sources
     * @param triplesMapFilter filter for specific TriplesMap instances; empty for all
     * @return a Flux of N-Quads encoded byte arrays
     */
    public Flux<byte[]> mapToNQuadsBytes(Map<String, InputStream> namedInputStreams, Set<TriplesMap> triplesMapFilter) {
        return mapLogicalViewsToBytes(namedInputStreams, triplesMapFilter, NTriplesTermEncoder.withDefaults(), true);
    }

    private Model toModel(Flux<Statement> statementFlux) {
        return statementFlux.collect(ModelCollector.toModel()).block(Duration.ofSeconds(SECONDS_TO_TIMEOUT));
    }
}
