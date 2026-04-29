package io.carml.logicalview;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.functions.FunctionRegistry;
import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.AbstractLogicalSource;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.ReferenceFormulation;
import io.carml.model.Source;
import io.carml.model.StructuralAnnotation;
import io.carml.model.UniqueAnnotation;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.util.CartesianProduct;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import reactor.core.publisher.Flux;

/**
 * Default implementation of {@link LogicalViewEvaluator} that evaluates a {@link LogicalView}
 * definition into a reactive stream of {@link ViewIteration}s. For each source record, it evaluates
 * {@link ExpressionField} entries (reference, template, constant) and recursively unnests
 * {@link IterableField} entries, building view iterations via Cartesian product expansion of
 * multivalued fields with absolute field name prefixing for nested fields.
 */
@Slf4j
public class DefaultLogicalViewEvaluator implements LogicalViewEvaluator {

    static final String INDEX_KEY = "#";

    static final String INDEX_KEY_SUFFIX = ".#";

    private final Set<MatchingLogicalSourceResolverFactory> resolverFactories;

    private final JoinExecutorFactory joinExecutorFactory;

    private final ExpressionMapEvaluator expressionMapValueEvaluator;

    public DefaultLogicalViewEvaluator(Set<MatchingLogicalSourceResolverFactory> resolverFactories) {
        this(
                resolverFactories,
                JoinExecutorFactory.inMemory(),
                new DefaultExpressionMapEvaluator(FunctionRegistry.create()));
    }

    public DefaultLogicalViewEvaluator(
            Set<MatchingLogicalSourceResolverFactory> resolverFactories, JoinExecutorFactory joinExecutorFactory) {
        this(resolverFactories, joinExecutorFactory, new DefaultExpressionMapEvaluator(FunctionRegistry.create()));
    }

    public DefaultLogicalViewEvaluator(
            Set<MatchingLogicalSourceResolverFactory> resolverFactories,
            JoinExecutorFactory joinExecutorFactory,
            ExpressionMapEvaluator expressionMapValueEvaluator) {
        this.resolverFactories = resolverFactories;
        this.joinExecutorFactory = joinExecutorFactory;
        this.expressionMapValueEvaluator = expressionMapValueEvaluator;
    }

    private record ExprEvalWithDatatypeMapper(ExpressionEvaluation exprEval, DatatypeMapper datatypeMapper) {}

    private record JoinOptimizations(boolean singleMatch, boolean eliminate, boolean effectiveInnerJoin) {}

    private record JoinContext(
            Function<Source, ResolvedSource<?>> sourceResolver,
            Set<LogicalViewJoin> aggregatingJoins,
            Map<LogicalView, List<ViewIteration>> parentViewCache,
            SourceRecordCache recordCache,
            Map<Source, Set<LogicalSource>> logicalSourcesPerSource,
            Map<LogicalSource, Set<String>> expressionsPerLogicalSource) {}

    /**
     * Groups the resolver caches, source record cache, and pre-collected LogicalSource info that
     * are threaded through {@code evaluateView} and {@code resolveExpressionEvaluations}.
     */
    private record EvaluationPipeline(
            Map<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryCache,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver,
            Function<ReferenceFormulation, Optional<Function<String, List<Object>>>> inlineRecordParserResolver,
            SourceRecordCache recordCache,
            Map<Source, Set<LogicalSource>> logicalSourcesPerSource,
            Map<LogicalSource, Set<String>> expressionsPerLogicalSource) {}

    private record FieldEvaluationContext(
            ReferenceFormulation currentRefForm,
            DatatypeMapper datatypeMapper,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver,
            Function<ReferenceFormulation, Optional<Function<String, List<Object>>>> inlineRecordParserResolver,
            ExpressionMapEvaluator expressionMapValueEvaluator) {}

    @Override
    public Flux<ViewIteration> evaluate(
            LogicalView view, Function<Source, ResolvedSource<?>> sourceResolver, EvaluationContext context) {
        return evaluate(view, sourceResolver, context, SourceRecordCache.noop(), Map.of(), Map.of());
    }

    @Override
    public Flux<ViewIteration> evaluate(
            LogicalView view,
            Function<Source, ResolvedSource<?>> sourceResolver,
            EvaluationContext context,
            SourceRecordCache recordCache,
            Map<Source, Set<LogicalSource>> logicalSourcesPerSource,
            Map<LogicalSource, Set<String>> expressionsPerLogicalSource) {

        var viewOn = view.getViewOn();

        if (!(viewOn instanceof LogicalSource) && !(viewOn instanceof LogicalView)) {
            throw new LogicalSourceResolverException(
                    "LogicalView viewOn must be a LogicalSource or LogicalView, but was %s"
                            .formatted(viewOn.getClass().getSimpleName()));
        }

        var rootLogicalSource = resolveRootLogicalSource(viewOn);
        var rootSource = rootLogicalSource.getSource();
        var rootRefForm = rootLogicalSource.getReferenceFormulation();

        // HashMap permits null keys, which is needed when rootRefForm is null.
        // Thread-safe: flatMapIterable processes elements sequentially, and all factory
        // resolution happens synchronously inside its lambda.
        var factoryCache =
                new HashMap<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>>();

        Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver =
                refForm ->
                        factoryCache.computeIfAbsent(refForm, rf -> resolveExpressionEvaluationFactory(rf, rootSource));

        var inlineRecordParserCache = new HashMap<ReferenceFormulation, Optional<Function<String, List<Object>>>>();
        Function<ReferenceFormulation, Optional<Function<String, List<Object>>>> inlineRecordParserResolver = refForm ->
                inlineRecordParserCache.computeIfAbsent(refForm, rf -> resolveInlineRecordParser(rf, rootSource));

        var pipeline = new EvaluationPipeline(
                factoryCache,
                factoryResolver,
                inlineRecordParserResolver,
                recordCache,
                logicalSourcesPerSource,
                expressionsPerLogicalSource);

        return evaluateView(view, sourceResolver, context, rootRefForm, pipeline);
    }

    private Flux<ViewIteration> evaluateView(
            LogicalView view,
            Function<Source, ResolvedSource<?>> sourceResolver,
            EvaluationContext context,
            ReferenceFormulation rootRefForm,
            EvaluationPipeline pipeline) {

        var exprEvalFlux = resolveExpressionEvaluations(view, sourceResolver, rootRefForm, pipeline);

        var evaluatedFlux = evaluateFieldsAndIndex(
                exprEvalFlux,
                view.getFields(),
                rootRefForm,
                context,
                pipeline.factoryResolver(),
                pipeline.inlineRecordParserResolver());

        evaluatedFlux = applyJoins(
                evaluatedFlux,
                view,
                context.getProjectedFields(),
                new JoinContext(
                        sourceResolver,
                        context.getAggregatingJoins(),
                        new HashMap<>(),
                        pipeline.recordCache(),
                        pipeline.logicalSourcesPerSource(),
                        pipeline.expressionsPerLogicalSource()));

        // Convert to ViewIteration — root # already assigned before joins
        Flux<ViewIteration> viewIterations = evaluatedFlux.map(ev -> new DefaultViewIteration(
                (int) ev.values().get(INDEX_KEY),
                ev.values(),
                ev.referenceFormulations(),
                ev.naturalDatatypes(),
                ev.sourceEvaluation()));

        // Apply dedup strategy — narrow key to projected fields when available
        var projectedFields = context.getProjectedFields();
        var keyFields =
                projectedFields.isEmpty() ? collectDedupKeyFields(view) : expandWithIndexKeys(projectedFields, view);
        var iterations = context.getDedupStrategy().deduplicate(viewIterations, keyFields);

        // Apply limit
        return context.getLimit().map(iterations::take).orElse(iterations);
    }

    @SuppressWarnings("unchecked")
    private Flux<ExprEvalWithDatatypeMapper> resolveExpressionEvaluations(
            LogicalView view,
            Function<Source, ResolvedSource<?>> sourceResolver,
            ReferenceFormulation rootRefForm,
            EvaluationPipeline pipeline) {

        var viewOn = view.getViewOn();

        if (viewOn instanceof LogicalSource logicalSource) {
            var rootSource = logicalSource.getSource();

            if (!pipeline.recordCache().isActive()) {
                // No caching — lazy streaming (original code path)
                var resolver = (LogicalSourceResolver<Object>) findResolver(logicalSource, rootSource);
                var expressionEvaluationFactory = resolver.getExpressionEvaluationFactory();
                pipeline.factoryCache().put(rootRefForm, expressionEvaluationFactory);

                LogicalSourceResolver.DatatypeMapperFactory<Object> datatypeMapperFactory =
                        resolver.getDatatypeMapperFactory().orElse(r -> value -> Optional.empty());

                var resolvedSource = sourceResolver.apply(rootSource);
                var expressions = collectViewExpressions(view, logicalSource);
                return resolver.getLogicalSourceRecords(Set.of(logicalSource), expressions)
                        .apply(resolvedSource)
                        .map(rec -> {
                            var sourceRecord = rec.getSourceRecord();
                            return new ExprEvalWithDatatypeMapper(
                                    expressionEvaluationFactory.apply(sourceRecord),
                                    datatypeMapperFactory.apply(sourceRecord));
                        });
            }

            // Thread-safe caching via Mono.cache()
            return pipeline.recordCache()
                    .getOrResolve(rootSource, () -> {
                        var resolver = (LogicalSourceResolver<Object>) findResolver(logicalSource, rootSource);
                        var exprEvalFactory = resolver.getExpressionEvaluationFactory();
                        LogicalSourceResolver.DatatypeMapperFactory<Object> dtMapperFactory =
                                resolver.getDatatypeMapperFactory().orElse(r -> value -> Optional.empty());
                        var resolvedSource = sourceResolver.apply(rootSource);
                        var allLogicalSources =
                                pipeline.logicalSourcesPerSource().getOrDefault(rootSource, Set.of(logicalSource));
                        return resolver.getLogicalSourceRecords(
                                        allLogicalSources, pipeline.expressionsPerLogicalSource())
                                .apply(resolvedSource)
                                .collectList()
                                .map(records -> new SourceRecordCache.CachedRecords(
                                        List.copyOf(records), exprEvalFactory, dtMapperFactory));
                    })
                    .flatMapMany(cached -> {
                        pipeline.factoryCache().put(rootRefForm, cached.expressionEvaluationFactory());
                        return filterAndMapRecords(
                                cached.records(),
                                logicalSource,
                                cached.expressionEvaluationFactory(),
                                cached.datatypeMapperFactory());
                    });
        }

        // view-on-view case: evaluate parent view recursively with cache
        var parentReferenceableKeys = collectReferenceableKeys((LogicalView) viewOn);
        return evaluateView((LogicalView) viewOn, sourceResolver, EvaluationContext.defaults(), rootRefForm, pipeline)
                .map(vi -> {
                    var viewExprEval = new ViewIterationExpressionEvaluation(vi, parentReferenceableKeys);
                    DatatypeMapper viewDatatypeMapper = vi::getNaturalDatatype;
                    return new ExprEvalWithDatatypeMapper(viewExprEval, viewDatatypeMapper);
                });
    }

    private static Flux<ExprEvalWithDatatypeMapper> filterAndMapRecords(
            List<LogicalSourceRecord<?>> records,
            LogicalSource logicalSource,
            LogicalSourceResolver.ExpressionEvaluationFactory<Object> expressionEvaluationFactory,
            LogicalSourceResolver.DatatypeMapperFactory<Object> datatypeMapperFactory) {
        return Flux.fromIterable(records)
                .filter(rec -> rec.getLogicalSource().equals(logicalSource))
                .map(rec -> {
                    var sourceRecord = (Object) rec.getSourceRecord();
                    return new ExprEvalWithDatatypeMapper(
                            expressionEvaluationFactory.apply(sourceRecord), datatypeMapperFactory.apply(sourceRecord));
                });
    }

    private Flux<EvaluatedValues> evaluateFieldsAndIndex(
            Flux<ExprEvalWithDatatypeMapper> exprEvalFlux,
            Set<Field> fields,
            ReferenceFormulation rootRefForm,
            EvaluationContext context,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver,
            Function<ReferenceFormulation, Optional<Function<String, List<Object>>>> inlineRecordParserResolver) {

        // Assign root # per source record BEFORE joins — so joined iterations share the
        // source record index. This preserves the source position semantics of # even when
        // joins expand one record into multiple iterations or filter records out.
        var sourceIndex = new AtomicInteger(0);
        return exprEvalFlux.flatMapIterable(pair -> {
            var idx = sourceIndex.getAndIncrement();
            var fieldCtx = new FieldEvaluationContext(
                    rootRefForm,
                    pair.datatypeMapper(),
                    factoryResolver,
                    inlineRecordParserResolver,
                    expressionMapValueEvaluator);
            var fieldEvals = evaluateFields(fields, pair.exprEval(), "", fieldCtx);
            var evaluatedStream = fieldEvals.stream();
            if (context.retainSourceEvaluation()) {
                var sourceEval = pair.exprEval();
                evaluatedStream = evaluatedStream.map(ev -> withSourceEvaluation(ev, sourceEval));
            }
            return evaluatedStream.map(ev -> withIndex(ev, INDEX_KEY, idx)).toList();
        });
    }

    private Flux<EvaluatedValues> applyJoins(
            Flux<EvaluatedValues> evaluatedFlux,
            LogicalView view,
            Set<String> projectedFields,
            JoinContext baseJoinCtx) {
        // Same-source optimization: when parent views share the child's LogicalSource and their
        // fields are all present in the child view, derive parent iterations from the child data
        // instead of re-reading the source. This requires materializing the child flux, then
        // deriving parents eagerly before join processing starts.
        var sameSourceParents = findSameSourceParentViews(view);

        if (!sameSourceParents.isEmpty()) {
            LOG.debug("Deriving {} same-source parent view(s) from child data", sameSourceParents.size());
            return evaluatedFlux.collectList().flatMapMany(childList -> {
                var parentViewCache = deriveParentIterations(childList, sameSourceParents);
                var joinCtx = new JoinContext(
                        baseJoinCtx.sourceResolver(),
                        baseJoinCtx.aggregatingJoins(),
                        parentViewCache,
                        baseJoinCtx.recordCache(),
                        baseJoinCtx.logicalSourcesPerSource(),
                        baseJoinCtx.expressionsPerLogicalSource());
                var childFlux = Flux.fromIterable(childList);
                childFlux = applyJoinSet(childFlux, view.getLeftJoins(), true, view, projectedFields, joinCtx);
                childFlux = applyJoinSet(childFlux, view.getInnerJoins(), false, view, projectedFields, joinCtx);
                return childFlux;
            });
        }

        var result = applyJoinSet(evaluatedFlux, view.getLeftJoins(), true, view, projectedFields, baseJoinCtx);
        result = applyJoinSet(result, view.getInnerJoins(), false, view, projectedFields, baseJoinCtx);
        return result;
    }

    /**
     * Finds parent views from the child view's joins that share the same {@link LogicalSource} and
     * whose fields are all present in the child view's fields. These parents can be derived from the
     * child data without re-reading the source.
     */
    private static Set<LogicalView> findSameSourceParentViews(LogicalView childView) {
        var childViewOn = childView.getViewOn();
        if (!(childViewOn instanceof LogicalSource)) {
            return Set.of();
        }

        var childFieldNames = childView.getFields().stream()
                .filter(ExpressionField.class::isInstance)
                .map(Field::getFieldName)
                .filter(Objects::nonNull)
                .collect(toUnmodifiableSet());

        var result = new LinkedHashSet<LogicalView>();
        collectSameSourceParents(childView.getLeftJoins(), childViewOn, childFieldNames, result);
        collectSameSourceParents(childView.getInnerJoins(), childViewOn, childFieldNames, result);
        return result;
    }

    private static void collectSameSourceParents(
            Set<LogicalViewJoin> joins,
            AbstractLogicalSource childViewOn,
            Set<String> childFieldNames,
            Set<LogicalView> result) {
        if (joins == null) {
            return;
        }
        for (var join : joins) {
            var parentView = join.getParentLogicalView();
            // Only optimize flat parent views (ExpressionFields only) sharing the same source.
            var allExpressionFields = parentView.getFields().stream().allMatch(ExpressionField.class::isInstance);
            if (childViewOn.equals(parentView.getViewOn()) && allExpressionFields) {
                var parentFieldNames = parentView.getFields().stream()
                        .map(Field::getFieldName)
                        .filter(Objects::nonNull)
                        .collect(toUnmodifiableSet());
                if (!parentFieldNames.isEmpty() && childFieldNames.containsAll(parentFieldNames)) {
                    result.add(parentView);
                }
            }
        }
    }

    /**
     * Derives parent {@link ViewIteration}s for same-source parent views from the materialized
     * child data. Returns a pre-populated cache that {@code applyJoin} will use to skip parent
     * source evaluation.
     */
    private static Map<LogicalView, List<ViewIteration>> deriveParentIterations(
            List<EvaluatedValues> childList, Set<LogicalView> sameSourceParents) {
        var cache = new HashMap<LogicalView, List<ViewIteration>>();
        for (var parentView : sameSourceParents) {
            var parentFieldKeys = collectReferenceableKeys(parentView);
            var parentIterations = childList.stream()
                    .map(ev -> projectToViewIteration(ev, parentFieldKeys))
                    .toList();
            cache.put(parentView, parentIterations);
        }
        return cache;
    }

    /**
     * Projects an {@link EvaluatedValues} into a {@link ViewIteration} containing only the fields
     * present in the given key set. Used to derive parent iterations from child data when both
     * share the same source.
     */
    private static ViewIteration projectToViewIteration(EvaluatedValues ev, Set<String> parentKeys) {
        var projectedValues = new LinkedHashMap<String, Object>();
        var projectedDatatypes = new LinkedHashMap<String, IRI>();
        var projectedRefFormulations = new LinkedHashMap<String, ReferenceFormulation>();
        for (var key : parentKeys) {
            if (ev.values().containsKey(key)) {
                projectedValues.put(key, ev.values().get(key));
            }
            if (ev.naturalDatatypes().containsKey(key)) {
                projectedDatatypes.put(key, ev.naturalDatatypes().get(key));
            }
            if (ev.referenceFormulations().containsKey(key)) {
                projectedRefFormulations.put(key, ev.referenceFormulations().get(key));
            }
        }
        var indexValue = ev.values().get(INDEX_KEY);
        var index = indexValue instanceof Number number ? number.intValue() : 0;
        projectedValues.put(INDEX_KEY, index);
        return new DefaultViewIteration(index, projectedValues, projectedRefFormulations, projectedDatatypes);
    }

    private Flux<EvaluatedValues> applyJoinSet(
            Flux<EvaluatedValues> evaluatedFlux,
            Set<LogicalViewJoin> joins,
            boolean leftJoin,
            LogicalView view,
            Set<String> projectedFields,
            JoinContext joinCtx) {
        if (joins == null) {
            return evaluatedFlux;
        }
        var result = evaluatedFlux;
        for (var join : joins) {
            var opts = analyzeJoinOptimizations(view, join, leftJoin, projectedFields);
            if (!opts.eliminate()) {
                result = applyJoin(
                        result,
                        join,
                        leftJoin && !opts.effectiveInnerJoin(),
                        opts.singleMatch(),
                        joinCtx.aggregatingJoins().contains(join),
                        joinCtx);
            }
        }
        return result;
    }

    private Set<String> collectDedupKeyFields(LogicalView view) {
        return collectAllFieldKeys(view);
    }

    private static Set<String> expandWithIndexKeys(Set<String> projectedFields, LogicalView view) {
        var allKeys = collectAllFieldKeys(view);
        var expanded = new LinkedHashSet<String>();
        for (var projectedField : projectedFields) {
            expanded.add(projectedField);
            var indexKey = projectedField + INDEX_KEY_SUFFIX;
            if (allKeys.contains(indexKey)) {
                expanded.add(indexKey);
            }
        }
        return expanded;
    }

    private static void collectFieldKeys(Set<Field> fields, String prefix, Set<String> keys) {
        if (fields == null) {
            return;
        }
        for (var field : fields) {
            if (field instanceof ExpressionField expressionField) {
                var absoluteName = prefix + expressionField.getFieldName();
                keys.add(absoluteName);
                keys.add(absoluteName + INDEX_KEY_SUFFIX);
                // Recurse into child fields for mixed-formulation support (ExpressionField with
                // child IterableFields using a different reference formulation, e.g., CSV column
                // containing JSON text or JSON field containing CSV text).
                var childFields = expressionField.getFields();
                if (childFields != null && !childFields.isEmpty()) {
                    collectFieldKeys(childFields, absoluteName + ".", keys);
                }
            } else if (field instanceof IterableField iterableField) {
                var absoluteName = prefix + iterableField.getFieldName();
                keys.add(absoluteName + INDEX_KEY_SUFFIX);
                collectFieldKeys(iterableField.getFields(), absoluteName + ".", keys);
            }
        }
    }

    private static void collectJoinFieldKeys(LogicalViewJoin join, Set<String> keys) {
        if (join.getFields() == null) {
            return;
        }
        for (var field : join.getFields()) {
            keys.add(field.getFieldName());
            keys.add(field.getFieldName() + INDEX_KEY_SUFFIX);
        }
    }

    private static LinkedHashSet<String> collectAllFieldKeys(LogicalView view) {
        var keys = new LinkedHashSet<String>();
        collectFieldKeys(view.getFields(), "", keys);
        if (view.getLeftJoins() != null) {
            for (var join : view.getLeftJoins()) {
                collectJoinFieldKeys(join, keys);
            }
        }
        if (view.getInnerJoins() != null) {
            for (var join : view.getInnerJoins()) {
                collectJoinFieldKeys(join, keys);
            }
        }
        return keys;
    }

    /**
     * Computes the set of referenceable keys for a given logical view definition. Referenceable keys
     * include expression field keys, index keys ({@code #}, {@code field.#}), and join field keys.
     * Iterable record keys and the root {@code <it>} key are excluded.
     */
    public static Set<String> collectReferenceableKeys(LogicalView view) {
        var keys = collectAllFieldKeys(view);
        keys.add(INDEX_KEY);
        return Set.copyOf(keys);
    }

    private LogicalSource resolveRootLogicalSource(AbstractLogicalSource abstractLogicalSource) {
        var current = abstractLogicalSource;
        while (current instanceof LogicalView lv) {
            current = lv.getViewOn();
        }
        if (current instanceof LogicalSource ls) {
            return ls;
        }
        throw new LogicalSourceResolverException(
                "Could not resolve root logical source from view chain; terminal viewOn was %s"
                        .formatted(current.getClass().getSimpleName()));
    }

    private LogicalSourceResolver<?> findResolver(LogicalSource logicalSource, Source source) {
        var matchedFactories = resolverFactories.stream()
                .map(factory -> factory.apply(logicalSource))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();

        if (matchedFactories.size() > 1) {
            LOG.debug(
                    "Found multiple matching resolvers for logical source: [{}]",
                    matchedFactories.stream()
                            .map(Object::getClass)
                            .map(Class::getSimpleName)
                            .collect(joining(", ")));
        }

        return MatchedLogicalSourceResolverFactory.select(matchedFactories)
                .orElseThrow(() -> new LogicalSourceResolverException(
                        "No logical source resolver found for reference formulation %s. Resolvers available: %s"
                                .formatted(
                                        logicalSource.getReferenceFormulation(),
                                        resolverFactories.stream()
                                                .map(MatchingLogicalSourceResolverFactory::getResolverName)
                                                .collect(joining(", ")))))
                .apply(source);
    }

    @SuppressWarnings("unchecked")
    private LogicalSourceResolver.ExpressionEvaluationFactory<Object> resolveExpressionEvaluationFactory(
            ReferenceFormulation referenceFormulation, Source source) {
        // Synthetic LogicalSource with only referenceFormulation set.
        // CarmlLogicalSource.getReferenceFormulation() returns the raw field
        // when source and tableName/sqlQuery are null.
        var syntheticLogicalSource = CarmlLogicalSource.builder()
                .referenceFormulation(referenceFormulation)
                .build();
        var resolver = (LogicalSourceResolver<Object>) findResolver(syntheticLogicalSource, source);
        resolver.configure(syntheticLogicalSource);
        return resolver.getExpressionEvaluationFactory();
    }

    private static List<EvaluatedValues> evaluateFields(
            Set<Field> fields, ExpressionEvaluation exprEval, String prefix, FieldEvaluationContext fieldCtx) {

        var fieldContributions = fields.stream()
                .sorted(Comparator.comparing(Field::getFieldName))
                .map(field -> {
                    if (field instanceof ExpressionField ef) {
                        var values = fieldCtx.expressionMapValueEvaluator()
                                .evaluate(ef, exprEval, fieldCtx.datatypeMapper());
                        var absoluteName = prefix + ef.getFieldName();
                        var indexKey = absoluteName + INDEX_KEY_SUFFIX;
                        var refFormMap = fieldCtx.currentRefForm() != null
                                ? Map.of(absoluteName, fieldCtx.currentRefForm())
                                : Map.<String, ReferenceFormulation>of();

                        // Resolve natural datatype for reference expressions
                        var naturalDatatypeMap =
                                resolveNaturalDatatypes(ef, absoluteName, indexKey, fieldCtx.datatypeMapper());

                        // Check for child fields (e.g., IterableField with a different ref formulation)
                        var childFields = ef.getFields();
                        if (childFields != null && !childFields.isEmpty()) {
                            return evaluateExpressionFieldWithChildren(
                                    values,
                                    absoluteName,
                                    indexKey,
                                    refFormMap,
                                    naturalDatatypeMap,
                                    childFields,
                                    fieldCtx);
                        }

                        if (values.isEmpty()) {
                            // Field evaluates to null — still contribute a single entry
                            // with the field key mapped to null and the index key set to 0,
                            // so the Cartesian product does not collapse the entire row.
                            // Use LinkedHashMap because Map.of() does not support null values.
                            var nullMap = new LinkedHashMap<String, Object>();
                            nullMap.put(absoluteName, null);
                            nullMap.put(indexKey, 0);
                            return List.of(new EvaluatedValues(nullMap, refFormMap, naturalDatatypeMap));
                        }

                        return IntStream.range(0, values.size())
                                .mapToObj(i -> new EvaluatedValues(
                                        Map.of(absoluteName, values.get(i), indexKey, i),
                                        refFormMap,
                                        naturalDatatypeMap))
                                .toList();
                    } else if (field instanceof IterableField itf) {
                        return evaluateIterableField(itf, exprEval, prefix, fieldCtx);
                    } else {
                        throw new UnsupportedOperationException("Unsupported field type: %s"
                                .formatted(field.getClass().getSimpleName()));
                    }
                })
                .toList();

        return CartesianProduct.listCartesianProduct(fieldContributions).stream()
                .map(DefaultLogicalViewEvaluator::mergeEvaluatedValues)
                .toList();
    }

    private static EvaluatedValues mergeEvaluatedValues(List<EvaluatedValues> parts) {
        var mergedValues = new LinkedHashMap<String, Object>();
        var mergedRefForms = new LinkedHashMap<String, ReferenceFormulation>();
        var mergedNaturalDatatypes = new LinkedHashMap<String, IRI>();
        // Preserve the source evaluation from the first part that has one (typically the child)
        ExpressionEvaluation sourceEval = null;
        for (var ev : parts) {
            mergedValues.putAll(ev.values());
            mergedRefForms.putAll(ev.referenceFormulations());
            mergedNaturalDatatypes.putAll(ev.naturalDatatypes());
            if (sourceEval == null && ev.sourceEvaluation() != null) {
                sourceEval = ev.sourceEvaluation();
            }
        }
        return new EvaluatedValues(mergedValues, mergedRefForms, mergedNaturalDatatypes, sourceEval);
    }

    private static List<EvaluatedValues> stampIndexOnSubRecords(
            List<Object> subRecords,
            String iterableIndexKey,
            LogicalSourceResolver.ExpressionEvaluationFactory<Object> nestedFactory,
            Set<Field> nestedFields,
            String subPrefix,
            FieldEvaluationContext fieldCtx) {
        return IntStream.range(0, subRecords.size())
                .boxed()
                .flatMap(i -> {
                    var subExprEval = nestedFactory.apply(subRecords.get(i));
                    return evaluateFields(nestedFields, subExprEval, subPrefix, fieldCtx).stream()
                            .map(ev -> withIndex(ev, iterableIndexKey, i));
                })
                .toList();
    }

    private static EvaluatedValues withIndex(EvaluatedValues ev, String indexKey, int index) {
        var values = new LinkedHashMap<>(ev.values());
        values.put(indexKey, index);
        var datatypes = new LinkedHashMap<>(ev.naturalDatatypes());
        datatypes.put(indexKey, XSD.INTEGER);
        return new EvaluatedValues(values, ev.referenceFormulations(), datatypes, ev.sourceEvaluation());
    }

    private static EvaluatedValues withSourceEvaluation(EvaluatedValues ev, ExpressionEvaluation sourceEval) {
        return new EvaluatedValues(ev.values(), ev.referenceFormulations(), ev.naturalDatatypes(), sourceEval);
    }

    private static Map<String, IRI> resolveNaturalDatatypes(
            ExpressionField ef, String absoluteName, String indexKey, DatatypeMapper datatypeMapper) {
        var naturalDatatypes = new LinkedHashMap<String, IRI>();

        // Index keys always have xsd:integer natural datatype
        naturalDatatypes.put(indexKey, XSD.INTEGER);

        // Natural datatypes are only resolved for reference expressions. Templates combine multiple
        // expressions (no single source datatype applies) and constants are string literals with no
        // source-determined datatype.
        if (ef.getReference() != null) {
            datatypeMapper.apply(ef.getReference()).ifPresent(iri -> naturalDatatypes.put(absoluteName, iri));
        }

        return naturalDatatypes;
    }

    private static List<EvaluatedValues> evaluateIterableField(
            IterableField field, ExpressionEvaluation exprEval, String prefix, FieldEvaluationContext fieldCtx) {

        var subRecords = exprEval.apply(field.getIterator())
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());

        if (subRecords.isEmpty()) {
            return List.of();
        }

        var nestedFields = field.getFields();
        if (nestedFields == null || nestedFields.isEmpty()) {
            return List.of();
        }

        var nestedRefForm = field.getReferenceFormulation();
        if (nestedRefForm == null && exprEval instanceof ViewIterationExpressionEvaluation viewExprEval) {
            nestedRefForm = viewExprEval
                    .getFieldReferenceFormulation(field.getIterator())
                    .orElse(null);
        }
        if (nestedRefForm == null) {
            nestedRefForm = fieldCtx.currentRefForm();
        }

        var nestedFactory = fieldCtx.factoryResolver().apply(nestedRefForm);
        return stampNestedSubRecords(field, prefix, subRecords, nestedRefForm, nestedFactory, nestedFields, fieldCtx);
    }

    /**
     * Evaluates an ExpressionField that has child fields (typically IterableFields with a different
     * reference formulation). For each expression value, merges the base ExpressionField evaluation
     * with the results of evaluating child fields via inline text parsing.
     */
    private static List<EvaluatedValues> evaluateExpressionFieldWithChildren(
            List<Object> values,
            String absoluteName,
            String indexKey,
            Map<String, ReferenceFormulation> refFormMap,
            Map<String, IRI> naturalDatatypeMap,
            Set<Field> childFields,
            FieldEvaluationContext fieldCtx) {

        var childPrefix = absoluteName + ".";

        return IntStream.range(0, values.size())
                .boxed()
                .flatMap(i -> {
                    var value = values.get(i);
                    var baseEv = new EvaluatedValues(
                            Map.of(absoluteName, value, indexKey, i), refFormMap, naturalDatatypeMap);

                    // Process child IterableFields via inline text parsing
                    var childContributions = childFields.stream()
                            .sorted(Comparator.comparing(Field::getFieldName))
                            .map(childField -> {
                                if (childField instanceof IterableField itf) {
                                    return evaluateInlineIterableField(itf, value, childPrefix, fieldCtx);
                                }
                                return List.<EvaluatedValues>of();
                            })
                            .toList();

                    var childCombos = CartesianProduct.listCartesianProduct(childContributions);

                    return childCombos.stream().map(combo -> {
                        var withBase = new ArrayList<EvaluatedValues>(combo.size() + 1);
                        withBase.add(baseEv);
                        withBase.addAll(combo);
                        return mergeEvaluatedValues(withBase);
                    });
                })
                .toList();
    }

    /**
     * Evaluates an IterableField that is a child of an ExpressionField, using inline text parsing
     * to convert the parent field's text value into the target format's native records. This handles
     * mixed reference formulations (e.g., CSV field containing JSON, or JSON field containing CSV).
     */
    private static List<EvaluatedValues> evaluateInlineIterableField(
            IterableField field, Object parentValue, String prefix, FieldEvaluationContext fieldCtx) {

        var nestedRefForm = field.getReferenceFormulation();
        if (nestedRefForm == null) {
            nestedRefForm = fieldCtx.currentRefForm();
        }

        if (parentValue == null) {
            return List.of();
        }

        // Parse the parent's text value into the target format's native records
        var effectiveNestedRefForm = nestedRefForm;
        var inlineRecordParser = fieldCtx.inlineRecordParserResolver()
                .apply(nestedRefForm)
                .orElseThrow(() -> new LogicalSourceResolverException(
                        ("No inline record parser available for reference formulation %s; "
                                        + "cannot evaluate iterable field with mixed reference formulations")
                                .formatted(effectiveNestedRefForm)));

        var parsedRecords = inlineRecordParser.apply(parentValue.toString());

        if (parsedRecords.isEmpty()) {
            return List.of();
        }

        var nestedFactory = fieldCtx.factoryResolver().apply(nestedRefForm);

        // If the iterable field has an iterator, apply it to extract sub-records from each parsed
        // record. For JSON: parse text → one JsonNode, apply "$[*]" → array elements.
        // If no iterator (e.g., CSV): each parsed record is already a sub-record.
        List<Object> subRecords;
        if (field.getIterator() != null) {
            subRecords = new ArrayList<>();
            for (var rec : parsedRecords) {
                var recordExprEval = nestedFactory.apply(rec);
                recordExprEval
                        .apply(field.getIterator())
                        .map(ExpressionEvaluation::extractValues)
                        .ifPresent(subRecords::addAll);
            }
        } else {
            subRecords = new ArrayList<>(parsedRecords);
        }

        if (subRecords.isEmpty()) {
            return List.of();
        }

        var nestedFields = field.getFields();
        if (nestedFields == null || nestedFields.isEmpty()) {
            return List.of();
        }

        return stampNestedSubRecords(field, prefix, subRecords, nestedRefForm, nestedFactory, nestedFields, fieldCtx);
    }

    /**
     * Builds the nested {@link FieldEvaluationContext}, prefixes, and index key for an iterable
     * sub-record traversal and delegates to {@link #stampIndexOnSubRecords}. Shared by
     * {@link #evaluateIterableField} and {@link #evaluateInlineIterableField} — both arrive at the
     * same shape (sub-records + nested fields + a resolved nested ref formulation and factory)
     * through different parsing paths.
     */
    private static List<EvaluatedValues> stampNestedSubRecords(
            IterableField field,
            String prefix,
            List<Object> subRecords,
            ReferenceFormulation nestedRefForm,
            LogicalSourceResolver.ExpressionEvaluationFactory<Object> nestedFactory,
            Set<Field> nestedFields,
            FieldEvaluationContext fieldCtx) {
        var subPrefix = prefix + field.getFieldName() + ".";
        var iterableIndexKey = prefix + field.getFieldName() + INDEX_KEY_SUFFIX;
        var nestedFieldCtx = new FieldEvaluationContext(
                nestedRefForm,
                fieldCtx.datatypeMapper(),
                fieldCtx.factoryResolver(),
                fieldCtx.inlineRecordParserResolver(),
                fieldCtx.expressionMapValueEvaluator());
        return stampIndexOnSubRecords(
                subRecords, iterableIndexKey, nestedFactory, nestedFields, subPrefix, nestedFieldCtx);
    }

    @SuppressWarnings("unchecked")
    private Optional<Function<String, List<Object>>> resolveInlineRecordParser(
            ReferenceFormulation referenceFormulation, Source source) {
        var syntheticLogicalSource = CarmlLogicalSource.builder()
                .referenceFormulation(referenceFormulation)
                .build();
        var resolver = (LogicalSourceResolver<Object>) findResolver(syntheticLogicalSource, source);
        resolver.configure(syntheticLogicalSource);
        return resolver.getInlineRecordParser();
    }

    private Flux<EvaluatedValues> applyJoin(
            Flux<EvaluatedValues> childFlux,
            LogicalViewJoin join,
            boolean isLeftJoin,
            boolean singleMatch,
            boolean isAggregating,
            JoinContext joinCtx) {

        var parentView = join.getParentLogicalView();
        var parentReferenceableKeys = collectReferenceableKeys(parentView);
        var conditions = join.getJoinConditions().stream()
                .sorted(Comparator.comparing(
                        c -> c.getChildMap().getExpressionMapExpressionSet().toString()))
                .toList();
        var sortedJoinFields = join.getFields().stream()
                .sorted(Comparator.comparing(ExpressionField::getFieldName))
                .toList();

        var cachedParent = joinCtx.parentViewCache().get(parentView);
        Flux<ViewIteration> parentFlux;
        if (cachedParent != null) {
            parentFlux = Flux.fromIterable(cachedParent);
        } else if (joinCtx.recordCache().isActive()) {
            parentFlux = evaluate(
                    parentView,
                    joinCtx.sourceResolver(),
                    EvaluationContext.defaults(),
                    joinCtx.recordCache(),
                    joinCtx.logicalSourcesPerSource(),
                    joinCtx.expressionsPerLogicalSource());
        } else {
            parentFlux = evaluate(parentView, joinCtx.sourceResolver(), EvaluationContext.defaults());
        }

        // For in-memory executors that already materialize the parent stream, write the
        // materialized list back into the per-mapping parent cache so subsequent joins to the
        // same parent view can reuse it without re-evaluating the source. Spillable executors
        // opt out via JoinExecutorFactory#cachesParentsInMemory() returning false — caching
        // there would defeat the spill-to-disk path's purpose.
        var finalParentFlux = cachedParent == null && joinExecutorFactory.cachesParentsInMemory()
                ? parentFlux
                        .collectList()
                        .doOnNext(list -> joinCtx.parentViewCache().put(parentView, list))
                        .flatMapIterable(Function.identity())
                : parentFlux;

        return Flux.using(
                () -> joinExecutorFactory.create(expressionMapValueEvaluator),
                executor -> executor.matches(
                                finalParentFlux, childFlux, conditions, parentReferenceableKeys, isLeftJoin)
                        .flatMapIterable(matched -> extendChildWithMatches(
                                matched,
                                sortedJoinFields,
                                join.getFields(),
                                singleMatch,
                                isLeftJoin,
                                isAggregating,
                                parentReferenceableKeys)),
                JoinExecutor::close);
    }

    private List<EvaluatedValues> extendChildWithMatches(
            MatchedRow matched,
            List<ExpressionField> sortedJoinFields,
            Set<ExpressionField> joinFields,
            boolean singleMatch,
            boolean isLeftJoin,
            boolean isAggregating,
            Set<String> parentReferenceableKeys) {
        var matchedParents = matched.matchedParents();
        if (matchedParents.isEmpty()) {
            return isLeftJoin ? List.of(extendWithNullJoinFields(matched.child(), joinFields)) : List.of();
        }
        var effectiveParents = singleMatch ? matchedParents.subList(0, 1) : matchedParents;
        return isAggregating
                ? matchAndExtendAggregating(
                        matched.child(), sortedJoinFields, effectiveParents, parentReferenceableKeys)
                : matchAndExtendRegular(matched.child(), sortedJoinFields, effectiveParents, parentReferenceableKeys);
    }

    private List<EvaluatedValues> matchAndExtendRegular(
            EvaluatedValues child,
            List<ExpressionField> sortedJoinFields,
            List<ViewIteration> effectiveParents,
            Set<String> parentReferenceableKeys) {

        var result = new ArrayList<EvaluatedValues>();
        // Track running index per join field across all parent matches so that
        // json_item.# counts 0, 1, 2, ... across parent iterations (not resetting per parent)
        var runningIndex = new LinkedHashMap<String, Integer>();
        for (var joinField : sortedJoinFields) {
            runningIndex.put(joinField.getFieldName(), 0);
        }

        for (var parentIteration : effectiveParents) {
            var parentExprEval = new ViewIterationExpressionEvaluation(parentIteration, parentReferenceableKeys);
            DatatypeMapper parentDatatypeMapper = parentIteration::getNaturalDatatype;

            // Evaluate join fields from the parent iteration, producing per-field value lists
            var fieldContributions = sortedJoinFields.stream()
                    .map(joinField -> {
                        var fieldName = joinField.getFieldName();
                        var indexKey = fieldName + INDEX_KEY_SUFFIX;
                        var values =
                                expressionMapValueEvaluator.evaluate(joinField, parentExprEval, parentDatatypeMapper);

                        // Resolve natural datatypes from parent iteration for join fields
                        var joinNaturalDatatypes = new LinkedHashMap<String, IRI>();
                        joinNaturalDatatypes.put(indexKey, XSD.INTEGER);
                        if (joinField.getReference() != null) {
                            parentIteration
                                    .getNaturalDatatype(joinField.getReference())
                                    .ifPresent(iri -> joinNaturalDatatypes.put(fieldName, iri));
                        }

                        if (values.isEmpty()) {
                            // Field evaluates to no values for this parent — still contribute a
                            // single entry with the field key mapped to null and the index key
                            // set to 0, so the Cartesian product does not collapse the entire
                            // join match. Mirrors the standalone-view branch in evaluateFields.
                            // Use LinkedHashMap because Map.of() does not support null values.
                            // Do NOT advance runningIndex — no values were emitted.
                            var nullValues = new LinkedHashMap<String, Object>();
                            nullValues.put(fieldName, null);
                            nullValues.put(indexKey, 0);
                            return List.of(new EvaluatedValues(nullValues, Map.of(), joinNaturalDatatypes));
                        }

                        var baseIdx = runningIndex.get(fieldName);
                        var evList = IntStream.range(0, values.size())
                                .mapToObj(i -> new EvaluatedValues(
                                        Map.of(fieldName, values.get(i), indexKey, baseIdx + i),
                                        Map.of(),
                                        joinNaturalDatatypes))
                                .toList();
                        runningIndex.put(fieldName, baseIdx + values.size());
                        return evList;
                    })
                    .toList();

            // Cartesian product of join field values, merged with child values
            var combinations = CartesianProduct.listCartesianProduct(fieldContributions);
            for (var combo : combinations) {
                var withChild = new ArrayList<EvaluatedValues>(combo.size() + 1);
                withChild.add(child);
                withChild.addAll(combo);
                result.add(mergeEvaluatedValues(withChild));
            }
        }

        return result;
    }

    /**
     * Aggregating match: instead of producing one row per matching parent, collects all matching
     * parent values into a single list per join field, yielding a single extended child row.
     */
    private List<EvaluatedValues> matchAndExtendAggregating(
            EvaluatedValues child,
            List<ExpressionField> sortedJoinFields,
            List<ViewIteration> effectiveParents,
            Set<String> parentReferenceableKeys) {

        var extendedValues = new LinkedHashMap<>(child.values());
        var extendedDatatypes = new LinkedHashMap<>(child.naturalDatatypes());

        for (var joinField : sortedJoinFields) {
            var fieldName = joinField.getFieldName();
            var allFieldValues = new ArrayList<>();

            for (var parentIteration : effectiveParents) {
                var parentExprEval = new ViewIterationExpressionEvaluation(parentIteration, parentReferenceableKeys);
                DatatypeMapper parentDatatypeMapper = parentIteration::getNaturalDatatype;
                var values = expressionMapValueEvaluator.evaluate(joinField, parentExprEval, parentDatatypeMapper);
                allFieldValues.addAll(values);
            }

            extendedValues.put(fieldName, allFieldValues.isEmpty() ? null : allFieldValues);
        }

        return List.of(new EvaluatedValues(
                extendedValues, child.referenceFormulations(), extendedDatatypes, child.sourceEvaluation()));
    }

    private static EvaluatedValues extendWithNullJoinFields(EvaluatedValues child, Set<ExpressionField> joinFields) {
        if (joinFields == null || joinFields.isEmpty()) {
            return child;
        }
        var extendedValues = new LinkedHashMap<>(child.values());
        for (var joinField : joinFields) {
            extendedValues.put(joinField.getFieldName(), null);
            extendedValues.put(joinField.getFieldName() + INDEX_KEY_SUFFIX, null);
        }
        return new EvaluatedValues(
                extendedValues, child.referenceFormulations(), child.naturalDatatypes(), child.sourceEvaluation());
    }

    private JoinOptimizations analyzeJoinOptimizations(
            LogicalView childView, LogicalViewJoin join, boolean isLeftJoin, Set<String> projectedFields) {
        var childAnnotations = nullSafeAnnotations(childView);
        var parentAnnotations = nullSafeAnnotations(join.getParentLogicalView());

        var singleMatch = isSingleMatchJoin(join, childAnnotations, parentAnnotations);
        var eliminate = isEliminableJoin(join, childAnnotations, projectedFields);
        var effectiveInnerJoin = isLeftJoin && isLeftToInnerConvertible(join, childAnnotations);

        return new JoinOptimizations(singleMatch, eliminate, effectiveInnerJoin);
    }

    private boolean isSingleMatchJoin(
            LogicalViewJoin join,
            Set<StructuralAnnotation> childAnnotations,
            Set<StructuralAnnotation> parentAnnotations) {
        var parentConditionFields = collectParentConditionFieldNames(join);

        // Path 1: Parent condition fields are covered by a PK
        if (hasPrimaryKeyCovering(parentAnnotations, parentConditionFields)) {
            return true;
        }

        // Path 2: FK on child referencing the parent view, and parent target fields are PK or Unique+NotNull
        for (var annotation : childAnnotations) {
            if (annotation instanceof ForeignKeyAnnotation fk && fk.getTargetView() == join.getParentLogicalView()) {
                var targetFieldNames = fieldNames(fk.getTargetFields());
                if (targetFieldNames.containsAll(parentConditionFields)
                        && parentConditionFields.containsAll(targetFieldNames)
                        && (hasPrimaryKeyCovering(parentAnnotations, targetFieldNames)
                                || hasUniqueOrPkCovering(parentAnnotations, targetFieldNames))) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean isEliminableJoin(
            LogicalViewJoin join, Set<StructuralAnnotation> childAnnotations, Set<String> projectedFields) {
        // Empty projectedFields means all fields are projected — cannot eliminate
        if (projectedFields.isEmpty()) {
            return false;
        }

        // Check for FK on child referencing the parent view
        boolean hasFk = childAnnotations.stream()
                .anyMatch(
                        a -> a instanceof ForeignKeyAnnotation fk && fk.getTargetView() == join.getParentLogicalView());
        if (!hasFk) {
            return false;
        }

        // Check that none of the join's data fields are in the projected set
        var joinDataFields = join.getFields();
        if (joinDataFields == null || joinDataFields.isEmpty()) {
            return true;
        }

        return joinDataFields.stream().map(ExpressionField::getFieldName).noneMatch(projectedFields::contains);
    }

    private boolean isLeftToInnerConvertible(LogicalViewJoin join, Set<StructuralAnnotation> childAnnotations) {
        var childConditionFields = collectChildConditionFieldNames(join);

        // All child condition fields must have NotNull annotations
        return childAnnotations.stream()
                .filter(NotNullAnnotation.class::isInstance)
                .anyMatch(nn -> {
                    var nnFields = nullSafeOnFields(nn);
                    return nnFields.containsAll(childConditionFields);
                });
    }

    private Set<String> collectChildConditionFieldNames(LogicalViewJoin join) {
        return join.getJoinConditions().stream()
                .flatMap(c -> c.getChildMap().getExpressionMapExpressionSet().stream())
                .collect(toUnmodifiableSet());
    }

    private Set<String> collectParentConditionFieldNames(LogicalViewJoin join) {
        return join.getJoinConditions().stream()
                .flatMap(c -> c.getParentMap().getExpressionMapExpressionSet().stream())
                .collect(toUnmodifiableSet());
    }

    private static Set<String> fieldNames(List<Field> fields) {
        if (fields == null) {
            return Set.of();
        }
        return fields.stream().map(Field::getFieldName).collect(toUnmodifiableSet());
    }

    private static boolean hasPrimaryKeyCovering(Set<StructuralAnnotation> annotations, Set<String> fieldNames) {
        return annotations.stream()
                .filter(PrimaryKeyAnnotation.class::isInstance)
                .anyMatch(pk -> {
                    var pkFields = nullSafeOnFields(pk);
                    return pkFields.containsAll(fieldNames) && fieldNames.containsAll(pkFields);
                });
    }

    private static boolean hasUniqueOrPkCovering(Set<StructuralAnnotation> annotations, Set<String> fieldNames) {
        boolean hasUniqueCovering = annotations.stream()
                .filter(a -> a instanceof UniqueAnnotation || a instanceof PrimaryKeyAnnotation)
                .anyMatch(u -> {
                    var uFields = nullSafeOnFields(u);
                    return uFields.containsAll(fieldNames) && fieldNames.containsAll(uFields);
                });
        if (!hasUniqueCovering) {
            return false;
        }

        // Also require NotNull on the same fields
        return annotations.stream().filter(NotNullAnnotation.class::isInstance).anyMatch(nn -> {
            var nnFields = nullSafeOnFields(nn);
            return nnFields.containsAll(fieldNames);
        });
    }

    private static Set<String> nullSafeOnFields(StructuralAnnotation annotation) {
        var fields = annotation.getOnFields();
        if (fields == null) {
            return Set.of();
        }
        return fields.stream().map(Field::getFieldName).collect(toUnmodifiableSet());
    }

    private static Set<StructuralAnnotation> nullSafeAnnotations(LogicalView view) {
        var annotations = view.getStructuralAnnotations();
        return annotations != null ? annotations : Set.of();
    }

    private static Map<LogicalSource, Set<String>> collectViewExpressions(
            LogicalView view, LogicalSource logicalSource) {
        var expressions = new LinkedHashSet<String>();
        collectFieldExpressions(view.getFields(), expressions);
        if (view.getLeftJoins() != null) {
            for (var join : view.getLeftJoins()) {
                join.getJoinConditions().stream()
                        .flatMap(c -> c.getChildMap().getExpressionMapExpressionSet().stream())
                        .forEach(expressions::add);
            }
        }
        if (view.getInnerJoins() != null) {
            for (var join : view.getInnerJoins()) {
                join.getJoinConditions().stream()
                        .flatMap(c -> c.getChildMap().getExpressionMapExpressionSet().stream())
                        .forEach(expressions::add);
            }
        }
        return Map.of(logicalSource, Set.copyOf(expressions));
    }

    private static void collectFieldExpressions(Set<Field> fields, Set<String> expressions) {
        if (fields == null) {
            return;
        }
        for (var field : fields) {
            if (field instanceof ExpressionField expressionField) {
                expressions.addAll(expressionField.getExpressionMapExpressionSet());
            }
            if (field instanceof IterableField iterableField) {
                collectFieldExpressions(iterableField.getFields(), expressions);
            }
        }
    }
}
