package io.carml.logicalview;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableSet;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.AbstractLogicalSource;
import io.carml.model.ExpressionField;
import io.carml.model.ExpressionMap;
import io.carml.model.Field;
import io.carml.model.ForeignKeyAnnotation;
import io.carml.model.IterableField;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.NotNullAnnotation;
import io.carml.model.PrimaryKeyAnnotation;
import io.carml.model.ReferenceFormulation;
import io.carml.model.Source;
import io.carml.model.StructuralAnnotation;
import io.carml.model.Template;
import io.carml.model.UniqueAnnotation;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import io.carml.util.CartesianProduct;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class DefaultLogicalViewEvaluator implements LogicalViewEvaluator {

    static final String INDEX_KEY = "#";

    static final String INDEX_KEY_SUFFIX = ".#";

    private final Set<MatchingLogicalSourceResolverFactory> resolverFactories;

    private record EvaluatedValues(
            Map<String, Object> values,
            Map<String, ReferenceFormulation> referenceFormulations,
            Map<String, IRI> naturalDatatypes) {}

    private record ExprEvalWithDatatypeMapper(ExpressionEvaluation exprEval, DatatypeMapper datatypeMapper) {}

    private record JoinOptimizations(boolean singleMatch, boolean eliminate, boolean effectiveInnerJoin) {}

    private record FieldEvaluationContext(
            ReferenceFormulation currentRefForm,
            DatatypeMapper datatypeMapper,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver,
            Function<ReferenceFormulation, Optional<Function<String, List<Object>>>> inlineRecordParserResolver) {}

    @Override
    public Flux<ViewIteration> evaluate(
            LogicalView view, Function<Source, ResolvedSource<?>> sourceResolver, EvaluationContext context) {

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

        return evaluateView(
                view, sourceResolver, context, rootRefForm, factoryCache, factoryResolver, inlineRecordParserResolver);
    }

    @SuppressWarnings("unchecked")
    private Flux<ViewIteration> evaluateView(
            LogicalView view,
            Function<Source, ResolvedSource<?>> sourceResolver,
            EvaluationContext context,
            ReferenceFormulation rootRefForm,
            Map<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryCache,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver,
            Function<ReferenceFormulation, Optional<Function<String, List<Object>>>> inlineRecordParserResolver) {

        var viewOn = view.getViewOn();
        var fields = view.getFields();

        Flux<ExprEvalWithDatatypeMapper> exprEvalFlux;
        if (viewOn instanceof LogicalSource logicalSource) {
            var rootSource = logicalSource.getSource();
            var resolver = (LogicalSourceResolver<Object>) findResolver(logicalSource, rootSource);
            var expressionEvaluationFactory = resolver.getExpressionEvaluationFactory();
            factoryCache.put(rootRefForm, expressionEvaluationFactory);

            LogicalSourceResolver.DatatypeMapperFactory<Object> datatypeMapperFactory =
                    resolver.getDatatypeMapperFactory().orElse(r -> value -> Optional.empty());

            var resolvedSource = sourceResolver.apply(rootSource);
            exprEvalFlux = resolver.getLogicalSourceRecords(Set.of(logicalSource))
                    .apply(resolvedSource)
                    .map(rec -> {
                        var sourceRecord = rec.getSourceRecord();
                        return new ExprEvalWithDatatypeMapper(
                                expressionEvaluationFactory.apply(sourceRecord),
                                datatypeMapperFactory.apply(sourceRecord));
                    });
        } else {
            var parentReferenceableKeys = collectReferenceableKeys((LogicalView) viewOn);
            exprEvalFlux = evaluateView(
                            (LogicalView) viewOn,
                            sourceResolver,
                            EvaluationContext.defaults(),
                            rootRefForm,
                            factoryCache,
                            factoryResolver,
                            inlineRecordParserResolver)
                    .map(vi -> {
                        var viewExprEval = new ViewIterationExpressionEvaluation(vi, parentReferenceableKeys);
                        DatatypeMapper viewDatatypeMapper = vi::getNaturalDatatype;
                        return new ExprEvalWithDatatypeMapper(viewExprEval, viewDatatypeMapper);
                    });
        }

        // Assign root # per source record BEFORE joins — so joined iterations share the
        // source record index. This preserves the source position semantics of # even when
        // joins expand one record into multiple iterations or filter records out.
        var sourceIndex = new AtomicInteger(0);
        Flux<EvaluatedValues> evaluatedFlux = exprEvalFlux.flatMapIterable(pair -> {
            var idx = sourceIndex.getAndIncrement();
            var fieldCtx = new FieldEvaluationContext(
                    rootRefForm, pair.datatypeMapper(), factoryResolver, inlineRecordParserResolver);
            var fieldEvals = evaluateFields(fields, pair.exprEval(), "", fieldCtx);
            return fieldEvals.stream().map(ev -> withIndex(ev, INDEX_KEY, idx)).toList();
        });

        // Apply joins: left joins first, then inner joins
        var leftJoins = view.getLeftJoins();
        var innerJoins = view.getInnerJoins();
        var projectedFields = context.getProjectedFields();

        if (leftJoins != null) {
            for (var join : leftJoins) {
                var opts = analyzeJoinOptimizations(view, join, true, projectedFields);
                if (opts.eliminate()) {
                    continue;
                }
                evaluatedFlux =
                        applyJoin(evaluatedFlux, join, !opts.effectiveInnerJoin(), opts.singleMatch(), sourceResolver);
            }
        }
        if (innerJoins != null) {
            for (var join : innerJoins) {
                var opts = analyzeJoinOptimizations(view, join, false, projectedFields);
                if (opts.eliminate()) {
                    continue;
                }
                evaluatedFlux = applyJoin(evaluatedFlux, join, false, opts.singleMatch(), sourceResolver);
            }
        }

        // Convert to ViewIteration — root # already assigned before joins
        Flux<ViewIteration> viewIterations = evaluatedFlux.map(ev -> new DefaultViewIteration(
                (int) ev.values().get(INDEX_KEY), ev.values(), ev.referenceFormulations(), ev.naturalDatatypes()));

        // Apply dedup strategy — narrow key to projected fields when available
        var keyFields =
                projectedFields.isEmpty() ? collectDedupKeyFields(view) : expandWithIndexKeys(projectedFields, view);
        var iterations = context.getDedupStrategy().deduplicate(viewIterations, keyFields);

        // Apply limit
        return context.getLimit().map(iterations::take).orElse(iterations);
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
    static Set<String> collectReferenceableKeys(LogicalView view) {
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
                        var values = evaluateExpressionMap(ef, exprEval);
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
        parts.forEach(ev -> {
            mergedValues.putAll(ev.values());
            mergedRefForms.putAll(ev.referenceFormulations());
            mergedNaturalDatatypes.putAll(ev.naturalDatatypes());
        });
        return new EvaluatedValues(mergedValues, mergedRefForms, mergedNaturalDatatypes);
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
        return new EvaluatedValues(values, ev.referenceFormulations(), datatypes);
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
        var subPrefix = prefix + field.getFieldName() + ".";
        var iterableIndexKey = prefix + field.getFieldName() + INDEX_KEY_SUFFIX;

        var nestedFieldCtx = new FieldEvaluationContext(
                nestedRefForm,
                fieldCtx.datatypeMapper(),
                fieldCtx.factoryResolver(),
                fieldCtx.inlineRecordParserResolver());

        return stampIndexOnSubRecords(
                subRecords, iterableIndexKey, nestedFactory, nestedFields, subPrefix, nestedFieldCtx);
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

        var subPrefix = prefix + field.getFieldName() + ".";
        var iterableIndexKey = prefix + field.getFieldName() + INDEX_KEY_SUFFIX;

        var nestedFieldCtx = new FieldEvaluationContext(
                nestedRefForm,
                fieldCtx.datatypeMapper(),
                fieldCtx.factoryResolver(),
                fieldCtx.inlineRecordParserResolver());

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
            Function<Source, ResolvedSource<?>> sourceResolver) {

        var parentView = join.getParentLogicalView();
        var parentReferenceableKeys = collectReferenceableKeys(parentView);
        var parentFlux = evaluate(parentView, sourceResolver, EvaluationContext.defaults());

        return parentFlux.collectList().flatMapMany(parentIterations -> {
            var conditions = join.getJoinConditions().stream()
                    .sorted(Comparator.comparing(
                            c -> c.getChildMap().getExpressionMapExpressionSet().toString()))
                    .toList();
            var joinIndex = buildJoinIndex(conditions, parentIterations, parentReferenceableKeys);
            return childFlux.flatMapIterable(child -> matchAndExtend(
                    child, conditions, join.getFields(), isLeftJoin, singleMatch, joinIndex, parentReferenceableKeys));
        });
    }

    private JoinIndex<List<Object>, ViewIteration> buildJoinIndex(
            List<Join> conditions, List<ViewIteration> parentIterations, Set<String> parentReferenceableKeys) {
        var joinIndex = new HashMapJoinIndex<List<Object>, ViewIteration>();
        for (var parentIteration : parentIterations) {
            var parentExprEval = new ViewIterationExpressionEvaluation(parentIteration, parentReferenceableKeys);
            var key = evaluateJoinKey(conditions, parentExprEval, true);
            if (!key.isEmpty()) {
                joinIndex.put(key, parentIteration);
            }
        }
        return joinIndex;
    }

    private List<Object> evaluateJoinKey(List<Join> conditions, ExpressionEvaluation exprEval, boolean isParent) {
        var key = new ArrayList<>(conditions.size());
        for (var condition : conditions) {
            var expressionMap = isParent ? condition.getParentMap() : condition.getChildMap();
            var values = evaluateExpressionMap(expressionMap, exprEval);
            if (values.isEmpty()) {
                return List.of();
            }
            // Use the first value of each condition as the key component
            key.add(values.get(0));
        }
        return key;
    }

    private List<EvaluatedValues> matchAndExtend(
            EvaluatedValues child,
            List<Join> conditions,
            Set<ExpressionField> joinFields,
            boolean isLeftJoin,
            boolean singleMatch,
            JoinIndex<List<Object>, ViewIteration> joinIndex,
            Set<String> parentReferenceableKeys) {

        // Build child key from EvaluatedValues — child field references point to field names in
        // child iteration values, so we use a simple lookup expression evaluation.
        ExpressionEvaluation childExprEval =
                expression -> Optional.ofNullable(child.values().get(expression));

        var childKey = evaluateJoinKey(conditions, childExprEval, false);
        if (childKey.isEmpty()) {
            return isLeftJoin ? List.of(extendWithNullJoinFields(child, joinFields)) : List.of();
        }

        var matchedParents = joinIndex.get(childKey);
        if (matchedParents.isEmpty()) {
            return isLeftJoin ? List.of(extendWithNullJoinFields(child, joinFields)) : List.of();
        }

        var effectiveParents = singleMatch ? matchedParents.subList(0, 1) : matchedParents;

        var sortedJoinFields = joinFields.stream()
                .sorted(Comparator.comparing(ExpressionField::getFieldName))
                .toList();

        var result = new ArrayList<EvaluatedValues>();
        // Track running index per join field across all parent matches so that
        // json_item.# counts 0, 1, 2, ... across parent iterations (not resetting per parent)
        var runningIndex = new LinkedHashMap<String, Integer>();
        for (var joinField : sortedJoinFields) {
            runningIndex.put(joinField.getFieldName(), 0);
        }

        for (var parentIteration : effectiveParents) {
            var parentExprEval = new ViewIterationExpressionEvaluation(parentIteration, parentReferenceableKeys);

            // Evaluate join fields from the parent iteration, producing per-field value lists
            var fieldContributions = sortedJoinFields.stream()
                    .map(joinField -> {
                        var fieldName = joinField.getFieldName();
                        var indexKey = fieldName + INDEX_KEY_SUFFIX;
                        var values = evaluateExpressionMap(joinField, parentExprEval);

                        // Resolve natural datatypes from parent iteration for join fields
                        var joinNaturalDatatypes = new LinkedHashMap<String, IRI>();
                        joinNaturalDatatypes.put(indexKey, XSD.INTEGER);
                        if (joinField.getReference() != null) {
                            parentIteration
                                    .getNaturalDatatype(joinField.getReference())
                                    .ifPresent(iri -> joinNaturalDatatypes.put(fieldName, iri));
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

    private static EvaluatedValues extendWithNullJoinFields(EvaluatedValues child, Set<ExpressionField> joinFields) {
        if (joinFields == null || joinFields.isEmpty()) {
            return child;
        }
        var extendedValues = new LinkedHashMap<>(child.values());
        for (var joinField : joinFields) {
            extendedValues.put(joinField.getFieldName(), null);
            extendedValues.put(joinField.getFieldName() + INDEX_KEY_SUFFIX, null);
        }
        return new EvaluatedValues(extendedValues, child.referenceFormulations(), child.naturalDatatypes());
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

    private static List<Object> evaluateExpressionMap(ExpressionMap field, ExpressionEvaluation exprEval) {
        if (field.getReference() != null) {
            return evaluateReference(field.getReference(), exprEval);
        }

        if (field.getTemplate() != null) {
            return evaluateTemplate(field.getTemplate(), exprEval);
        }

        if (field.getConstant() != null) {
            return List.of(field.getConstant().stringValue());
        }

        if (field.getFunctionExecution() != null) {
            throw new UnsupportedOperationException("Function execution in expression fields not yet supported");
        }

        return List.of();
    }

    private static List<Object> evaluateReference(String reference, ExpressionEvaluation exprEval) {
        return exprEval.apply(reference)
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private static List<Object> evaluateTemplate(Template template, ExpressionEvaluation exprEval) {
        var segmentValueLists = new ArrayList<List<String>>();

        for (var segment : template.getSegments()) {
            if (segment instanceof TextSegment textSegment) {
                segmentValueLists.add(List.of(textSegment.getValue()));
            } else if (segment instanceof ExpressionSegment expressionSegment) {
                var values = exprEval.apply(expressionSegment.getValue())
                        .map(ExpressionEvaluation::extractStringValues)
                        .orElse(List.of());

                if (values.isEmpty()) {
                    return List.of();
                }

                segmentValueLists.add(values);
            } else {
                throw new UnsupportedOperationException("Unsupported template segment type: %s"
                        .formatted(segment.getClass().getSimpleName()));
            }
        }

        var segmentCombinations = CartesianProduct.listCartesianProduct(segmentValueLists);

        return segmentCombinations.stream()
                .map(combination -> (Object) String.join("", combination))
                .toList();
    }
}
