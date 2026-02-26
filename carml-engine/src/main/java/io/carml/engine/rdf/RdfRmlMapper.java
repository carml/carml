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
import io.carml.engine.function.AnnotatedFunctionProvider;
import io.carml.engine.function.BuiltInFunctionProvider;
import io.carml.engine.function.FnoDescriptionProvider;
import io.carml.engine.function.FunctionDescriptor;
import io.carml.engine.function.FunctionProvider;
import io.carml.engine.function.FunctionRegistry;
import io.carml.engine.function.ParameterDescriptor;
import io.carml.engine.function.ReturnDescriptor;
import io.carml.engine.join.ChildSideJoinStoreProvider;
import io.carml.engine.join.ParentSideJoinConditionStoreProvider;
import io.carml.engine.join.impl.CarmlChildSideJoinStoreProvider;
import io.carml.engine.join.impl.CarmlParentSideJoinConditionStoreProvider;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.FileResolver;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolver;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseConnectionOptions;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseSourceResolver;
import io.carml.model.Mapping;
import io.carml.model.TriplesMap;
import io.carml.util.Mappings;
import io.carml.util.TypeRef;
import java.io.InputStream;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
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

    private final MappingExecutionObserver observer;

    private RdfRmlMapper(
            Set<TriplesMap> triplesMaps,
            MappingPipeline<Statement> mappingPipeline,
            Set<SourceResolver<?>> sourceResolvers,
            List<ResolvedMapping> resolvedMappings,
            MappingExecutionObserver observer) {
        super(triplesMaps, mappingPipeline, sourceResolvers, resolvedMappings);
        this.observer = observer;
    }

    /**
     * Returns the observer associated with this mapper. Useful for downstream components that need
     * to fire observer callbacks (e.g., view evaluation wiring).
     *
     * @return the mapping execution observer, never {@code null}
     */
    public MappingExecutionObserver getObserver() {
        return observer;
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

        private Supplier<ValueFactory> valueFactorySupplier = SimpleValueFactory::getInstance;

        private Normalizer.Form normalizationForm = Normalizer.Form.NFC;

        private boolean iriUpperCasePercentEncoding = true;

        private TermGeneratorFactory<Value> termGeneratorFactory;

        private ChildSideJoinStoreProvider<MappedValue<Resource>, MappedValue<IRI>> childSideJoinCacheProvider =
                CarmlChildSideJoinStoreProvider.of();

        private ParentSideJoinConditionStoreProvider<MappedValue<Resource>> parentSideJoinConditionStoreProvider =
                CarmlParentSideJoinConditionStoreProvider.of();

        private DatabaseConnectionOptions databaseConnectionOptions;

        private final List<MappingExecutionObserver> observers = new ArrayList<>();

        private boolean strictMode = false;

        private Long limit;

        private Set<String> excludeLogicalSourceResolvers = new HashSet<>();

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

        public Builder childSideJoinStoreProvider(
                ChildSideJoinStoreProvider<MappedValue<Resource>, MappedValue<IRI>> childSideJoinCacheProvider) {
            this.childSideJoinCacheProvider = childSideJoinCacheProvider;
            return this;
        }

        public Builder parentSideJoinConditionStoreProvider(
                ParentSideJoinConditionStoreProvider<MappedValue<Resource>> parentSideJoinConditionStoreProvider) {
            this.parentSideJoinConditionStoreProvider = parentSideJoinConditionStoreProvider;
            return this;
        }

        // Will override all connections
        public Builder databaseConnectionOptions(DatabaseConnectionOptions databaseConnectionOptions) {
            this.databaseConnectionOptions = databaseConnectionOptions;
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
            var matchingLogicalSourceResolverFactories =
                    ServiceLoader.load(MatchingLogicalSourceResolverFactory.class).stream()
                            .map(ServiceLoader.Provider::get)
                            .filter(not(factory -> excludeLogicalSourceResolvers.contains(factory.getResolverName())))
                            .collect(toUnmodifiableSet());

            if (matchingLogicalSourceResolverFactories.isEmpty()) {
                throw new RmlMapperException("No logical source resolver suppliers specified.");
            }

            var triplesMaps = mapping != null ? mapping.getTriplesMaps() : providedTriplesMaps;
            if (triplesMaps == null) {
                throw new RmlMapperException("No mappings provided.");
            }

            var functionRegistry = buildFunctionRegistry();

            RdfTermGeneratorConfig rdfTermGeneratorConfig = RdfTermGeneratorConfig.builder()
                    .baseIri(baseIri)
                    .valueFactory(valueFactorySupplier.get())
                    .normalizationForm(normalizationForm)
                    .iriUpperCasePercentEncoding(iriUpperCasePercentEncoding)
                    .functionRegistry(functionRegistry)
                    .build();

            var rdfMapperConfig = RdfMapperConfig.builder()
                    .valueFactorySupplier(valueFactorySupplier)
                    .termGeneratorFactory(termGeneratorFactory)
                    .rdfTermGeneratorConfig(rdfTermGeneratorConfig)
                    .childSideJoinStoreProvider(childSideJoinCacheProvider)
                    .parentSideJoinConditionStoreProvider(parentSideJoinConditionStoreProvider)
                    .strictMode(strictMode)
                    .build();

            var pipelineFactory = RdfMappingPipelineFactory.getInstance();

            var mappableTriplesMaps = Mappings.filterMappable(triplesMaps);
            var resolvedMappings = MappingResolver.resolve(mappableTriplesMaps, limit);

            var observer = CompositeObserver.of(observers);

            var mappingPipeline = pipelineFactory.getMappingPipeline(
                    mappableTriplesMaps, rdfMapperConfig, matchingLogicalSourceResolverFactories);

            // Wire each TriplesMapper with its corresponding ResolvedMapping for error context
            // enrichment via FieldOrigin provenance, and with the observer for pipeline callbacks.
            for (var resolvedMapping : resolvedMappings) {
                mappingPipeline.getTriplesMappers().stream()
                        .filter(RdfTriplesMapper.class::isInstance)
                        .map(RdfTriplesMapper.class::cast)
                        .filter(rtm -> rtm.getTriplesMap().equals(resolvedMapping.getOriginalTriplesMap()))
                        .findFirst()
                        .ifPresent(rtm -> {
                            rtm.setResolvedMapping(resolvedMapping);
                            rtm.setObserver(observer);
                        });
            }

            sourceResolvers.add(fileResolverBuilder.build());

            if (databaseConnectionOptions != null) {
                sourceResolvers.add(DatabaseSourceResolver.of(databaseConnectionOptions));
            } else {
                sourceResolvers.add(DatabaseSourceResolver.of());
            }

            ServiceLoader.load(SourceResolver.class).stream()
                    .<SourceResolver<?>>map(ServiceLoader.Provider::get)
                    .forEach(sourceResolvers::add);

            return new RdfRmlMapper(triplesMaps, mappingPipeline, sourceResolvers, resolvedMappings, observer);
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
            for (var fnoModel : pendingFnoDescriptions) {
                registry.registerAll(new FnoDescriptionProvider(fnoModel));
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

    private Model toModel(Flux<Statement> statementFlux) {
        return statementFlux.collect(ModelCollector.toModel()).block(Duration.ofSeconds(SECONDS_TO_TIMEOUT));
    }
}
