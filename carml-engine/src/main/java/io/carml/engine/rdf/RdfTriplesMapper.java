package io.carml.engine.rdf;

import static io.carml.util.LogUtil.exception;
import static io.carml.vocab.Rdf.Rr;
import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.engine.ByteMappingResult;
import io.carml.engine.FieldOrigin;
import io.carml.engine.MappedValue;
import io.carml.engine.MappingError;
import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.MappingResult;
import io.carml.engine.MergeableMappingResult;
import io.carml.engine.NoOpObserver;
import io.carml.engine.NonExistentReferenceException;
import io.carml.engine.ResolvedMapping;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapper;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.rdf.cc.RdfContainer;
import io.carml.engine.rdf.cc.RdfList;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalview.ViewIteration;
import io.carml.logicalview.ViewIterationExpressionEvaluation;
import io.carml.logicalview.ViewIterationExpressionEvaluationException;
import io.carml.model.GraphMap;
import io.carml.model.LogicalSource;
import io.carml.model.RefObjectMap;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.output.NTriplesTermEncoder;
import io.carml.vocab.Rdf.Rml;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class RdfTriplesMapper<R> implements TriplesMapper<Statement> {

    static final UnaryOperator<Resource> defaultGraphModifier =
            graph -> graph.equals(Rr.defaultGraph) || graph.equals(Rml.defaultGraph) ? null : graph;

    static final Consumer<Statement> logAddStatements = statement -> {
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Adding statement {} {} {} {} to result set",
                    statement.getSubject(),
                    statement.getPredicate(),
                    statement.getObject(),
                    statement.getContext());
        }
    };

    @NonNull
    private final TriplesMap triplesMap;

    private final Set<RdfSubjectMapper> subjectMappers;

    private final Set<RdfPredicateObjectMapper> predicateObjectMappers;

    @NonNull
    private final LogicalSourceResolver.ExpressionEvaluationFactory<R> expressionEvaluationFactory;

    private final LogicalSourceResolver.DatatypeMapperFactory<R> datatypeMapperFactory;

    private final boolean strictMode;

    private final Set<String> referenceExpressions;

    private final Set<String> matchedExpressions;

    private volatile boolean recordsProcessed;

    private volatile boolean viewIterationUsed;

    private final AtomicBoolean mappingStartFired = new AtomicBoolean(false);

    private ResolvedMapping resolvedMapping;

    // Set once during build(), before any Flux subscription — visibility is guaranteed by the
    // Reactor subscription barrier (same pattern as resolvedMapping).
    private MappingExecutionObserver observer = NoOpObserver.getInstance();

    private RdfTriplesMapper(
            @NonNull TriplesMap triplesMap,
            Set<RdfSubjectMapper> subjectMappers,
            Set<RdfPredicateObjectMapper> predicateObjectMappers,
            @NonNull LogicalSourceResolver.ExpressionEvaluationFactory<R> expressionEvaluationFactory,
            LogicalSourceResolver.DatatypeMapperFactory<R> datatypeMapperFactory,
            boolean strictMode,
            Set<String> referenceExpressions,
            Set<String> matchedExpressions) {
        this.triplesMap = triplesMap;
        this.subjectMappers = subjectMappers;
        this.predicateObjectMappers = predicateObjectMappers;
        this.expressionEvaluationFactory = expressionEvaluationFactory;
        this.datatypeMapperFactory = datatypeMapperFactory;
        this.strictMode = strictMode;
        this.referenceExpressions = referenceExpressions;
        this.matchedExpressions = matchedExpressions;
    }

    public static <R> RdfTriplesMapper<R> of(
            @NonNull TriplesMap triplesMap,
            @NonNull LogicalSourceResolver<R> logicalSourceResolver,
            @NonNull RdfMapperConfig rdfMapperConfig) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating mapper for TriplesMap {}", triplesMap.getResourceName());
        }

        var subjectMappers = createSubjectMappers(triplesMap, rdfMapperConfig);

        var predicateObjectMappers = createPredicateObjectMappers(triplesMap, rdfMapperConfig);

        var isStrictMode = rdfMapperConfig.isStrictMode();
        var refExpressions = isStrictMode ? triplesMap.getReferenceExpressionSet() : Set.<String>of();
        var matched = isStrictMode ? TrackingExpressionEvaluation.createMatchedExpressionsSet() : Set.<String>of();

        var baseFactory = logicalSourceResolver.getExpressionEvaluationFactory();
        LogicalSourceResolver.ExpressionEvaluationFactory<R> effectiveFactory = isStrictMode
                ? sourceRecord -> TrackingExpressionEvaluation.of(baseFactory.apply(sourceRecord), matched)
                : baseFactory;

        return new RdfTriplesMapper<>(
                triplesMap,
                subjectMappers,
                predicateObjectMappers,
                effectiveFactory,
                logicalSourceResolver.getDatatypeMapperFactory().orElse(null),
                isStrictMode,
                refExpressions,
                matched);
    }

    /**
     * Creates a {@link RdfTriplesMapper} for a LogicalView-based TriplesMap. LV mappers only use
     * {@link #map(ViewIteration)}; the {@link LogicalSourceResolver.ExpressionEvaluationFactory}
     * is a no-op stub and must never be invoked.
     *
     * @param triplesMap           the LV-based TriplesMap
     * @param rdfMapperConfig      the mapper configuration
     * @param refObjectMapPrefixes mapping from RefObjectMaps to their expression prefixes for
     *                             joined RefObjectMap resolution via view left joins
     * @return a mapper wired for view iteration mapping only
     */
    static RdfTriplesMapper<Object> ofForView(
            @NonNull TriplesMap triplesMap,
            @NonNull RdfMapperConfig rdfMapperConfig,
            Map<RefObjectMap, String> refObjectMapPrefixes) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating LV mapper for TriplesMap {}", triplesMap.getResourceName());
        }

        // Create a view-specific RdfTermGeneratorFactory with refObjectMapPrefixes so that
        // gather maps with joined RefObjectMaps can resolve parent expressions via view left joins
        var baseFactory = (RdfTermGeneratorFactory) rdfMapperConfig.getTermGeneratorFactory();
        var rdfTermGeneratorFactory = refObjectMapPrefixes.isEmpty()
                ? baseFactory
                : baseFactory.withRefObjectMapPrefixes(refObjectMapPrefixes);

        var subjectMappers = createSubjectMappers(triplesMap, rdfMapperConfig);
        var predicateObjectMappers = triplesMap.getPredicateObjectMaps().stream()
                .map(pom -> RdfPredicateObjectMapper.forView(
                        pom, triplesMap, rdfMapperConfig, rdfTermGeneratorFactory, refObjectMapPrefixes))
                .collect(toUnmodifiableSet());

        // No-op factory — never invoked; only map(ViewIteration) is called on LV mappers.
        LogicalSourceResolver.ExpressionEvaluationFactory<Object> noOpFactory =
                sourceRecord -> expression -> Optional.empty();

        return new RdfTriplesMapper<>(
                triplesMap,
                subjectMappers,
                predicateObjectMappers,
                noOpFactory,
                null, // no datatype mapper factory
                false,
                Set.of(),
                Set.of());
    }

    /**
     * Creates a {@link RdfTriplesMapper} for a LogicalView-based TriplesMap with a specific subset
     * of predicate-object maps and class triple emission control. Used for view decomposition where
     * each group of POMs is evaluated separately.
     *
     * @param triplesMap the LV-based TriplesMap
     * @param rdfMapperConfig the mapper configuration
     * @param refObjectMapPrefixes mapping from RefObjectMaps to their expression prefixes
     * @param activePredicateObjectMaps the subset of POMs to evaluate; must not be empty
     * @param emitsClassTriples whether this mapper should emit rdf:type class triples
     * @return a mapper wired for view iteration mapping with the given POM subset
     */
    static RdfTriplesMapper<Object> ofForView(
            @NonNull TriplesMap triplesMap,
            @NonNull RdfMapperConfig rdfMapperConfig,
            Map<RefObjectMap, String> refObjectMapPrefixes,
            Set<io.carml.model.PredicateObjectMap> activePredicateObjectMaps,
            boolean emitsClassTriples) {

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating decomposed LV mapper for TriplesMap {} with {} POMs, emitsClassTriples={}",
                    triplesMap.getResourceName(),
                    activePredicateObjectMaps.size(),
                    emitsClassTriples);
        }

        var baseFactory = (RdfTermGeneratorFactory) rdfMapperConfig.getTermGeneratorFactory();
        var rdfTermGeneratorFactory = refObjectMapPrefixes.isEmpty()
                ? baseFactory
                : baseFactory.withRefObjectMapPrefixes(refObjectMapPrefixes);

        var subjectMappers = emitsClassTriples
                ? createSubjectMappers(triplesMap, rdfMapperConfig)
                : triplesMap.getSubjectMaps().stream()
                        .map(sm -> RdfSubjectMapper.ofWithoutClasses(sm, triplesMap, rdfMapperConfig))
                        .collect(toUnmodifiableSet());

        var predicateObjectMappers = activePredicateObjectMaps.stream()
                .map(pom -> RdfPredicateObjectMapper.forView(
                        pom, triplesMap, rdfMapperConfig, rdfTermGeneratorFactory, refObjectMapPrefixes))
                .collect(toUnmodifiableSet());

        LogicalSourceResolver.ExpressionEvaluationFactory<Object> noOpFactory =
                sourceRecord -> expression -> Optional.empty();

        return new RdfTriplesMapper<>(
                triplesMap, subjectMappers, predicateObjectMappers, noOpFactory, null, false, Set.of(), Set.of());
    }

    static Set<TermGenerator<Resource>> createGraphGenerators(
            Set<GraphMap> graphMaps, RdfTermGeneratorFactory termGeneratorFactory) {
        return graphMaps.stream().map(termGeneratorFactory::getGraphGenerator).collect(toUnmodifiableSet());
    }

    @SuppressWarnings("java:S3864")
    private static Set<RdfSubjectMapper> createSubjectMappers(TriplesMap triplesMap, RdfMapperConfig rdfMapperConfig) {

        Set<SubjectMap> subjectMaps = triplesMap.getSubjectMaps();
        if (subjectMaps.isEmpty()) {
            throw new TriplesMapperException(String.format(
                    "Subject map must be specified in triples map %s", exception(triplesMap, triplesMap)));
        }

        if (subjectMaps.size() > 1 && !rdfMapperConfig.isAllowMultipleSubjectMaps()) {
            throw new TriplesMapperException("TriplesMap %s has %s subject maps, but only one is allowed. "
                            .formatted(exception(triplesMap, triplesMap), subjectMaps.size())
                    + "Use allowMultipleSubjectMaps(true) to enable multiple subject maps.");
        }

        return subjectMaps.stream()
                .peek(sm -> LOG.debug("Creating mapper for SubjectMap {}", sm.getResourceName()))
                .map(sm -> RdfSubjectMapper.of(sm, triplesMap, rdfMapperConfig))
                .collect(toUnmodifiableSet());
    }

    @SuppressWarnings("java:S3864")
    private static Set<RdfPredicateObjectMapper> createPredicateObjectMappers(
            TriplesMap triplesMap, RdfMapperConfig rdfMapperConfig) {
        return triplesMap.getPredicateObjectMaps().stream()
                .peek(pom -> LOG.debug("Creating mapper for PredicateObjectMap {}", pom.getResourceName()))
                .map(pom -> RdfPredicateObjectMapper.of(pom, triplesMap, rdfMapperConfig))
                .collect(toUnmodifiableSet());
    }

    @Override
    public @org.jspecify.annotations.NonNull TriplesMap getTriplesMap() {
        return triplesMap;
    }

    @Override
    public LogicalSource getLogicalSource() {
        return (LogicalSource) triplesMap.getLogicalSource();
    }

    /**
     * Associates this mapper with a {@link ResolvedMapping} for error context enrichment. When set,
     * errors from {@link #map(ViewIteration)} are enriched with {@link FieldOrigin} provenance
     * information, producing user-facing error messages that reference the original mapping
     * constructs.
     *
     * @param resolvedMapping the resolved mapping providing field origin context
     */
    void setResolvedMapping(ResolvedMapping resolvedMapping) {
        this.resolvedMapping = resolvedMapping;
    }

    /**
     * Associates this mapper with a {@link MappingExecutionObserver} for pipeline callbacks.
     * Statement generation and error events are forwarded to the observer.
     *
     * @param observer the observer to receive callbacks
     */
    void setObserver(MappingExecutionObserver observer) {
        this.observer = observer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Flux<MappingResult<Statement>> map(LogicalSourceRecord<?> logicalSourceRecord) {
        var sourceRecord = (R) logicalSourceRecord.getSourceRecord();
        LOG.trace("Mapping triples for record {}", logicalSourceRecord);
        fireMappingStartOnce();
        recordsProcessed = true;
        var expressionEvaluation = expressionEvaluationFactory.apply(sourceRecord);
        var datatypeMapper = datatypeMapperFactory != null ? datatypeMapperFactory.apply(sourceRecord) : null;

        return mapEvaluation(expressionEvaluation, datatypeMapper)
                .map(result -> instrumentWithObserver(result, null))
                .doOnError(ex -> fireError(null, ex));
    }

    @Override
    public Flux<MappingResult<Statement>> map(ViewIteration viewIteration) {
        LOG.trace("Mapping triples for view iteration {}", viewIteration.getIndex());
        var expressionEvaluation = prepareViewIterationEvaluation(viewIteration);
        return mapEvaluation(expressionEvaluation, viewIteration::getNaturalDatatype)
                .map(result -> instrumentWithObserver(result, viewIteration))
                .doOnError(ex -> fireError(viewIteration, ex));
    }

    /**
     * Returns a {@link MappingResult} whose inner results publisher fires
     * {@link MappingExecutionObserver#onStatementGenerated} for every emitted {@link Statement}
     * as a side effect of subscription. When no observer or no resolved mapping is configured,
     * the original result is returned unchanged — the NoOp path is a simple identity that avoids
     * adding any reactive operators on the hot path.
     *
     * <p>{@link MergeableMappingResult} instances (per-iteration pieces of rdf:List / rdf:Container
     * results that will be merged across iterations) are returned unchanged here. Wrapping them
     * would produce an {@link ObserverFiringMappingResult} which is a {@code MappingResult}
     * (not a {@code MergeableMappingResult}), breaking the {@code instanceof MergeableMappingResult}
     * check in {@code RmlMapper.handleCompletable} that captures them for merging.
     * The observer-firing wrap for the merged tail happens later at
     * {@code RmlMapper.wrapMergedForObserver} (overridden in
     * {@code RdfRmlMapper}) — merged rdf:List / rdf:Container statements DO fire the observer
     * there, with {@code null} for both {@link ResolvedMapping} and the view iteration (merged
     * results aggregate across iterations, often across decomposed sub-mappings).
     *
     * <p>The {@code source} iteration is {@code null} for the legacy {@link LogicalSourceRecord}
     * path. Observers inspecting {@code source} must tolerate that (e.g.
     * {@link io.carml.engine.LoggingObserver#onStatementGenerated} guards against it).
     *
     * <p><strong>Per-term-map provenance:</strong> the wrapped {@link MappingResult} carries the
     * exact union of {@link io.carml.model.LogicalTarget}s declared on the subject, predicate,
     * object and (optional) graph term maps that produced each statement. That union is surfaced
     * to the observer via {@link ObserverFiringMappingResult}; no heuristic attribution is
     * performed by this mapper.
     *
     * <p>Visibility: package-private to permit direct unit testing of the
     * {@link MergeableMappingResult} bypass branch. Not part of the public engine API.
     */
    MappingResult<Statement> instrumentWithObserver(MappingResult<Statement> mappingResult, ViewIteration source) {
        if (resolvedMapping == null || observer == NoOpObserver.getInstance()) {
            return mappingResult;
        }
        if (mappingResult instanceof MergeableMappingResult<?, ?>) {
            return mappingResult;
        }
        return new ObserverFiringMappingResult<>(mappingResult, resolvedMapping, source, observer);
    }

    /**
     * Shared setup for both {@link #map(ViewIteration)} and {@link #mapToBytes}: fires the
     * mapping-start event, marks iteration state, builds the expression evaluation chain
     * (view evaluation → source fallback → error enrichment).
     */
    private ExpressionEvaluation prepareViewIterationEvaluation(ViewIteration viewIteration) {
        fireMappingStartOnce();
        recordsProcessed = true;
        viewIterationUsed = true;
        // Use schema-level referenceable keys for validation when available; otherwise fall back
        // to the row's actual keys. Schema-level keys enable strict validation of field references,
        // catching typos and invalid references (e.g., "Name" when the view field is "name").
        var referenceableKeys = viewIteration.getReferenceableKeys().orElse(viewIteration.getKeys());
        var viewEvaluation = new ViewIterationExpressionEvaluation(viewIteration, referenceableKeys);
        // Fall back to the source-level expression evaluation for expressions not captured as
        // view fields (e.g., gather map references that must retain multi-valued results).
        ExpressionEvaluation baseEvaluation = viewIteration
                .getSourceEvaluation()
                .map(sourceEval -> withSourceFallback(viewEvaluation, sourceEval))
                .orElse(viewEvaluation);
        return resolvedMapping != null ? enrichingEvaluation(baseEvaluation) : baseEvaluation;
    }

    private static ExpressionEvaluation withSourceFallback(
            ViewIterationExpressionEvaluation viewEvaluation, ExpressionEvaluation sourceEvaluation) {
        return expression -> {
            try {
                return viewEvaluation.apply(expression);
            } catch (ViewIterationExpressionEvaluationException ex) {
                return sourceEvaluation.apply(expression);
            }
        };
    }

    private void fireMappingStartOnce() {
        if (resolvedMapping != null && mappingStartFired.compareAndSet(false, true)) {
            observer.onMappingStart(resolvedMapping);
        }
    }

    void fireError(ViewIteration iteration, Throwable ex) {
        if (resolvedMapping != null) {
            observer.onError(resolvedMapping, iteration, MappingError.of(ex.getMessage(), ex));
        }
    }

    private ExpressionEvaluation enrichingEvaluation(ExpressionEvaluation baseEvaluation) {
        return expression -> {
            try {
                return baseEvaluation.apply(expression);
            } catch (RuntimeException ex) {
                throw enrichError(ex, expression);
            }
        };
    }

    private RuntimeException enrichError(RuntimeException original, String expression) {
        var origin = resolvedMapping.getFieldOrigin(expression);
        if (origin.isEmpty()) {
            return original;
        }

        var fieldOrigin = origin.get();
        var originatingTriplesMap = fieldOrigin.getOriginatingTriplesMap();

        var location = fieldOrigin
                .getOriginatingTermMap()
                .map(termMap -> exception(originatingTriplesMap, termMap))
                .orElse("TriplesMap %s".formatted(originatingTriplesMap.getResourceName()));

        String message;
        if (resolvedMapping.isImplicitView()) {
            message = "Error evaluating reference '%s' in %s".formatted(fieldOrigin.getOriginalExpression(), location);
        } else {
            message = "Error evaluating field '%s' (reference '%s') in %s"
                    .formatted(expression, fieldOrigin.getOriginalExpression(), location);
        }

        return new TriplesMapperException(message, original);
    }

    @Override
    public Flux<MappingResult<Statement>> mapEvaluation(
            ExpressionEvaluation expressionEvaluation, DatatypeMapper datatypeMapper) {

        var subjectMapperResults = subjectMappers.stream()
                .map(subjectMapper -> subjectMapper.map(expressionEvaluation, datatypeMapper))
                .collect(toUnmodifiableSet());

        var subjects = subjectMapperResults.stream()
                .map(RdfSubjectMapper.Result::getSubjects)
                .flatMap(Set::stream)
                .collect(toUnmodifiableSet());

        if (subjects.isEmpty()) {
            return Flux.empty();
        }

        Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndSubjectGraphs = new HashMap<>();
        List<Flux<MappingResult<Statement>>> subjectStatementFluxes = new ArrayList<>();

        for (RdfSubjectMapper.Result subjectMapperResult : subjectMapperResults) {
            var resultSubjects = subjectMapperResult.getSubjects();
            if (!resultSubjects.isEmpty()) {
                subjectsAndSubjectGraphs.put(resultSubjects, subjectMapperResult.getGraphs());
                subjectStatementFluxes.add(subjectMapperResult.getTypeStatements());
            }
        }

        var subjectStatements = Flux.merge(subjectStatementFluxes);
        var pomStatements = Flux.fromIterable(predicateObjectMappers)
                .flatMap(predicateObjectMapper ->
                        predicateObjectMapper.map(expressionEvaluation, datatypeMapper, subjectsAndSubjectGraphs));

        return Flux.merge(subjectStatements, pomStatements);
    }

    @Override
    public ByteMappingResult<Statement> mapToBytes(
            ViewIteration viewIteration, NTriplesTermEncoder encoder, boolean includeGraph) {
        LOG.trace("Byte-mapping triples for view iteration {}", viewIteration.getIndex());
        var expressionEvaluation = prepareViewIterationEvaluation(viewIteration);
        try {
            return mapEvaluationToBytes(expressionEvaluation, viewIteration::getNaturalDatatype, encoder, includeGraph);
        } catch (RuntimeException ex) {
            fireError(viewIteration, ex);
            throw ex;
        }
    }

    private ByteMappingResult<Statement> mapEvaluationToBytes(
            ExpressionEvaluation expressionEvaluation,
            DatatypeMapper datatypeMapper,
            NTriplesTermEncoder encoder,
            boolean includeGraph) {

        var subjectMapperResults = subjectMappers.stream()
                .map(subjectMapper -> subjectMapper.map(expressionEvaluation, datatypeMapper))
                .collect(toUnmodifiableSet());

        var subjects = subjectMapperResults.stream()
                .map(RdfSubjectMapper.Result::getSubjects)
                .flatMap(Set::stream)
                .collect(toUnmodifiableSet());

        if (subjects.isEmpty()) {
            return new ByteMappingResult<>(List.of(), List.of());
        }

        Map<Set<MappedValue<Resource>>, Set<MappedValue<Resource>>> subjectsAndSubjectGraphs = new HashMap<>();
        var allBytes = new ArrayList<byte[]>();
        var allMergeables = new ArrayList<MappingResult<Statement>>();

        for (RdfSubjectMapper.Result subjectMapperResult : subjectMapperResults) {
            var resultSubjects = subjectMapperResult.getSubjects();
            if (!resultSubjects.isEmpty()) {
                subjectsAndSubjectGraphs.put(resultSubjects, subjectMapperResult.getGraphs());

                // Encode type statements to bytes
                allBytes.addAll(subjectMappers.stream()
                        .flatMap(sm -> sm
                                .encodeTypeStatements(
                                        resultSubjects, subjectMapperResult.getGraphs(), encoder, includeGraph)
                                .stream())
                        .toList());

                // Handle collection subjects (RdfList/RdfContainer) -- encode their Statements.
                // toStream() is safe here: RdfList/RdfContainer results are synchronous
                // (backed by Flux.fromIterable over an in-memory Model).
                resultSubjects.stream()
                        .filter(s -> s instanceof RdfList || s instanceof RdfContainer)
                        .forEach(collSubject -> {
                            var collResult =
                                    collSubject instanceof RdfList<?> rdfList ? rdfList : (RdfContainer<?>) collSubject;
                            Flux.from(collResult.getResults())
                                    .toStream()
                                    .map(stmt -> encodeStatement(stmt, encoder, includeGraph))
                                    .forEach(allBytes::add);
                        });
            }
        }

        // Encode POM results to bytes
        for (var pomMapper : predicateObjectMappers) {
            allBytes.addAll(pomMapper.mapToBytes(
                    expressionEvaluation,
                    datatypeMapper,
                    subjectsAndSubjectGraphs,
                    encoder,
                    allMergeables,
                    includeGraph));
        }

        return new ByteMappingResult<>(allBytes, allMergeables);
    }

    /**
     * Encodes a single Statement to bytes, respecting the includeGraph flag.
     */
    private static byte[] encodeStatement(Statement stmt, NTriplesTermEncoder encoder, boolean includeGraph) {
        if (includeGraph) {
            return encoder.encodeNQuad(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
        }
        return encoder.encodeNTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
    }

    @Override
    public Mono<Void> checkStrictModeExpressions() {
        if (!strictMode || !recordsProcessed || viewIterationUsed) {
            return Mono.empty();
        }

        var unmatchedExpressions = referenceExpressions.stream()
                .filter(expr -> !matchedExpressions.contains(expr))
                .sorted()
                .toList();

        if (unmatchedExpressions.isEmpty()) {
            return Mono.empty();
        }

        var message = String.format(
                "The following reference expression(s) in TriplesMap %s never produced a non-null result: %s",
                triplesMap.getResourceName(), String.join(", ", unmatchedExpressions));

        return Mono.error(new NonExistentReferenceException(message));
    }
}
