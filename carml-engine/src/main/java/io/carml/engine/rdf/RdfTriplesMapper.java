package io.carml.engine.rdf;

import static io.carml.util.LogUtil.exception;
import static io.carml.vocab.Rdf.Rr;
import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.engine.FieldOrigin;
import io.carml.engine.MappedValue;
import io.carml.engine.MappingError;
import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.MappingResult;
import io.carml.engine.NoOpObserver;
import io.carml.engine.NonExistentReferenceException;
import io.carml.engine.RefObjectMapper;
import io.carml.engine.ResolvedMapping;
import io.carml.engine.TermGenerator;
import io.carml.engine.TriplesMapper;
import io.carml.engine.TriplesMapperException;
import io.carml.engine.join.ParentSideJoinConditionStore;
import io.carml.engine.join.ParentSideJoinKey;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalview.ViewIteration;
import io.carml.logicalview.ViewIterationExpressionEvaluation;
import io.carml.model.GraphMap;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.SubjectMap;
import io.carml.model.TriplesMap;
import io.carml.vocab.Rdf.Rml;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
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

    private final Set<RdfRefObjectMapper> incomingRefObjectMappers;

    @NonNull
    private final LogicalSourceResolver.ExpressionEvaluationFactory<R> expressionEvaluationFactory;

    private final LogicalSourceResolver.DatatypeMapperFactory<R> datatypeMapperFactory;

    @NonNull
    private final ParentSideJoinConditionStore<MappedValue<Resource>> parentSideJoinConditions;

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
            Set<RdfRefObjectMapper> incomingRefObjectMappers,
            @NonNull LogicalSourceResolver.ExpressionEvaluationFactory<R> expressionEvaluationFactory,
            LogicalSourceResolver.DatatypeMapperFactory<R> datatypeMapperFactory,
            @NonNull ParentSideJoinConditionStore<MappedValue<Resource>> parentSideJoinConditions,
            boolean strictMode,
            Set<String> referenceExpressions,
            Set<String> matchedExpressions) {
        this.triplesMap = triplesMap;
        this.subjectMappers = subjectMappers;
        this.predicateObjectMappers = predicateObjectMappers;
        this.incomingRefObjectMappers = incomingRefObjectMappers;
        this.expressionEvaluationFactory = expressionEvaluationFactory;
        this.datatypeMapperFactory = datatypeMapperFactory;
        this.parentSideJoinConditions = parentSideJoinConditions;
        this.strictMode = strictMode;
        this.referenceExpressions = referenceExpressions;
        this.matchedExpressions = matchedExpressions;
    }

    public static <R> RdfTriplesMapper<R> of(
            @NonNull TriplesMap triplesMap,
            Set<RdfRefObjectMapper> refObjectMappers,
            Set<RdfRefObjectMapper> incomingRefObjectMappers,
            @NonNull LogicalSourceResolver<R> logicalSourceResolver,
            @NonNull RdfMapperConfig rdfMapperConfig) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating mapper for TriplesMap {}", triplesMap.getResourceName());
        }

        var subjectMappers = createSubjectMappers(triplesMap, rdfMapperConfig);

        var predicateObjectMappers = createPredicateObjectMappers(triplesMap, rdfMapperConfig, refObjectMappers);

        Set<RdfRefObjectMapper> actionableIncomingRefObjectMappers;
        if (triplesMap.getLogicalTable() != null) {
            actionableIncomingRefObjectMappers = incomingRefObjectMappers.stream()
                    .filter(rom -> rom.getTriplesMap().getLogicalTable() != null)
                    .collect(toUnmodifiableSet());
        } else {
            actionableIncomingRefObjectMappers = incomingRefObjectMappers;
        }

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
                actionableIncomingRefObjectMappers,
                effectiveFactory,
                logicalSourceResolver.getDatatypeMapperFactory().orElse(null),
                rdfMapperConfig
                        .getParentSideJoinConditionStoreProvider()
                        .createParentSideJoinConditionStore(triplesMap.getId()),
                isStrictMode,
                refExpressions,
                matched);
    }

    /**
     * Creates a {@link RdfTriplesMapper} for a LogicalView-based TriplesMap. LV mappers only use
     * {@link #map(ViewIteration)}; the {@link LogicalSourceResolver.ExpressionEvaluationFactory}
     * is a no-op stub and must never be invoked.
     *
     * @param triplesMap the LV-based TriplesMap
     * @param rdfMapperConfig the mapper configuration
     * @return a mapper wired for view iteration mapping only
     */
    static RdfTriplesMapper<Object> ofForView(
            @NonNull TriplesMap triplesMap, @NonNull RdfMapperConfig rdfMapperConfig) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating LV mapper for TriplesMap {}", triplesMap.getResourceName());
        }

        var subjectMappers = createSubjectMappers(triplesMap, rdfMapperConfig);
        var predicateObjectMappers = createPredicateObjectMappers(triplesMap, rdfMapperConfig, Set.of());

        // No-op factory — never invoked; only map(ViewIteration) is called on LV mappers.
        LogicalSourceResolver.ExpressionEvaluationFactory<Object> noOpFactory =
                sourceRecord -> expression -> Optional.empty();

        return new RdfTriplesMapper<>(
                triplesMap,
                subjectMappers,
                predicateObjectMappers,
                Set.of(), // no incoming ref object mappers — LV joins handled by evaluator
                noOpFactory,
                null, // no datatype mapper factory
                rdfMapperConfig
                        .getParentSideJoinConditionStoreProvider()
                        .createParentSideJoinConditionStore(triplesMap.getId()),
                false,
                Set.of(),
                Set.of());
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

        return subjectMaps.stream()
                .peek(sm -> LOG.debug("Creating mapper for SubjectMap {}", sm.getResourceName()))
                .map(sm -> RdfSubjectMapper.of(sm, triplesMap, rdfMapperConfig))
                .collect(toUnmodifiableSet());
    }

    @SuppressWarnings("java:S3864")
    private static Set<RdfPredicateObjectMapper> createPredicateObjectMappers(
            TriplesMap triplesMap, RdfMapperConfig rdfMapperConfig, Set<RdfRefObjectMapper> refObjectMappers) {
        return triplesMap.getPredicateObjectMaps().stream()
                .peek(pom -> LOG.debug("Creating mapper for PredicateObjectMap {}", pom.getResourceName()))
                .map(pom -> RdfPredicateObjectMapper.of(pom, triplesMap, refObjectMappers, rdfMapperConfig))
                .collect(toUnmodifiableSet());
    }

    @Override
    public TriplesMap getTriplesMap() {
        return triplesMap;
    }

    @Override
    public LogicalSource getLogicalSource() {
        return (LogicalSource) triplesMap.getLogicalSource();
    }

    Set<RefObjectMapper<Statement>> getRefObjectMappers() {
        return predicateObjectMappers.stream()
                .flatMap(pom -> pom.getRdfRefObjectMappers().stream())
                .collect(toUnmodifiableSet());
    }

    Set<RefObjectMapper<Statement>> getConnectedRefObjectMappers() {
        return Stream.concat(getRefObjectMappers().stream(), incomingRefObjectMappers.stream())
                .collect(toUnmodifiableSet());
    }

    @Override
    public ParentSideJoinConditionStore<MappedValue<Resource>> getParentSideJoinConditions() {
        return parentSideJoinConditions;
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

        return mapEvaluation(expressionEvaluation, datatypeMapper).doOnError(ex -> fireError(null, ex));
    }

    @Override
    public Flux<MappingResult<Statement>> map(ViewIteration viewIteration) {
        LOG.trace("Mapping triples for view iteration {}", viewIteration.getIndex());
        fireMappingStartOnce();
        recordsProcessed = true;
        viewIterationUsed = true;
        // Strict mode tracking is not applied here — ViewIterationExpressionEvaluation validates
        // referenced keys eagerly in apply(), so unmatched references are caught immediately.
        var baseEvaluation = new ViewIterationExpressionEvaluation(viewIteration, viewIteration.getKeys());
        var expressionEvaluation = resolvedMapping != null ? enrichingEvaluation(baseEvaluation) : baseEvaluation;
        return mapEvaluation(expressionEvaluation, null).doOnError(ex -> fireError(viewIteration, ex));
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

        cacheParentSideJoinConditions(expressionEvaluation, subjects);

        return Flux.merge(subjectStatements, pomStatements);
    }

    private void cacheParentSideJoinConditions(
            ExpressionEvaluation expressionEvaluation, Set<MappedValue<Resource>> subjects) {
        incomingRefObjectMappers.forEach(incomingRefObjectMapper -> incomingRefObjectMapper
                .getRefObjectMap()
                .getJoinConditions()
                .forEach(join -> processJoinCondition(join, expressionEvaluation, subjects)));
    }

    private void processJoinCondition(
            Join join, ExpressionEvaluation expressionEvaluation, Set<MappedValue<Resource>> subjects) {
        // TODO
        String parentReference = join.getParentMap().getReference();

        expressionEvaluation
                .apply(parentReference)
                .ifPresent(referenceResult -> ExpressionEvaluation.extractStringValues(referenceResult)
                        .forEach(parentValue ->
                                processJoinConditionParentValue(subjects, parentReference, parentValue)));
    }

    private void processJoinConditionParentValue(
            Set<MappedValue<Resource>> subjects, String parentReference, String parentValue) {
        ParentSideJoinKey parentSideJoinKey = ParentSideJoinKey.of(parentReference, parentValue);
        Set<MappedValue<Resource>> parentSubjects = new HashSet<>(subjects);

        if (parentSideJoinConditions.containsKey(parentSideJoinKey)) {
            // merge incoming subjects with already cached subjects for key
            parentSubjects.addAll(parentSideJoinConditions.get(parentSideJoinKey));
        }

        parentSideJoinConditions.put(ParentSideJoinKey.of(parentReference, parentValue), parentSubjects);
    }

    @Override
    public Mono<Void> validate() {
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

    public void cleanup() {
        parentSideJoinConditions.clear();
    }
}
