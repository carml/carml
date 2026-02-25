package io.carml.logicalview;

import static java.util.stream.Collectors.joining;

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
import io.carml.model.IterableField;
import io.carml.model.Join;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.model.ReferenceFormulation;
import io.carml.model.Source;
import io.carml.model.Template;
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

        return evaluateView(view, sourceResolver, context, rootRefForm, factoryCache, factoryResolver);
    }

    @SuppressWarnings("unchecked")
    private Flux<ViewIteration> evaluateView(
            LogicalView view,
            Function<Source, ResolvedSource<?>> sourceResolver,
            EvaluationContext context,
            ReferenceFormulation rootRefForm,
            Map<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryCache,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver) {

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
            exprEvalFlux = evaluateView(
                            (LogicalView) viewOn, sourceResolver, context, rootRefForm, factoryCache, factoryResolver)
                    .map(vi -> {
                        var viewExprEval = new ViewIterationExpressionEvaluation(vi);
                        DatatypeMapper viewDatatypeMapper = vi::getNaturalDatatype;
                        return new ExprEvalWithDatatypeMapper(viewExprEval, viewDatatypeMapper);
                    });
        }

        Flux<EvaluatedValues> evaluatedFlux = exprEvalFlux.flatMapIterable(pair ->
                evaluateFields(fields, pair.exprEval(), pair.datatypeMapper(), rootRefForm, "", factoryResolver));

        // Apply joins: left joins first, then inner joins
        var leftJoins = view.getLeftJoins();
        var innerJoins = view.getInnerJoins();

        if (leftJoins != null) {
            for (var join : leftJoins) {
                evaluatedFlux = applyJoin(evaluatedFlux, join, true, sourceResolver);
            }
        }
        if (innerJoins != null) {
            for (var join : innerJoins) {
                evaluatedFlux = applyJoin(evaluatedFlux, join, false, sourceResolver);
            }
        }

        // Convert to ViewIteration without root # for dedup
        Flux<ViewIteration> viewIterations = evaluatedFlux.map(
                ev -> new DefaultViewIteration(0, ev.values(), ev.referenceFormulations(), ev.naturalDatatypes()));

        // Apply dedup strategy
        var keyFields = collectDedupKeyFields(view);
        viewIterations = context.getDedupStrategy().deduplicate(viewIterations, keyFields);

        // Assign sequential # index after dedup
        var index = new AtomicInteger(0);
        var iterations = viewIterations.map(
                vi -> (ViewIteration) ((DefaultViewIteration) vi).withIndex(index.getAndIncrement()));

        // Apply limit
        return context.getLimit().map(iterations::take).orElse(iterations);
    }

    private Set<String> collectDedupKeyFields(LogicalView view) {
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

    private void collectFieldKeys(Set<Field> fields, String prefix, Set<String> keys) {
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

    private void collectJoinFieldKeys(LogicalViewJoin join, Set<String> keys) {
        if (join.getFields() == null) {
            return;
        }
        for (var field : join.getFields()) {
            keys.add(field.getFieldName());
            keys.add(field.getFieldName() + INDEX_KEY_SUFFIX);
        }
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
        return resolver.getExpressionEvaluationFactory();
    }

    private List<EvaluatedValues> evaluateFields(
            Set<Field> fields,
            ExpressionEvaluation exprEval,
            DatatypeMapper datatypeMapper,
            ReferenceFormulation currentRefForm,
            String prefix,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver) {

        var fieldContributions = fields.stream()
                .sorted(Comparator.comparing(Field::getFieldName))
                .map(field -> {
                    if (field instanceof ExpressionField ef) {
                        var values = evaluateExpressionMap(ef, exprEval);
                        var absoluteName = prefix + ef.getFieldName();
                        var indexKey = absoluteName + INDEX_KEY_SUFFIX;
                        var refFormMap = currentRefForm != null
                                ? Map.of(absoluteName, currentRefForm)
                                : Map.<String, ReferenceFormulation>of();

                        // Resolve natural datatype for reference expressions
                        var naturalDatatypeMap = resolveNaturalDatatypes(ef, absoluteName, indexKey, datatypeMapper);

                        return IntStream.range(0, values.size())
                                .mapToObj(i -> new EvaluatedValues(
                                        Map.of(absoluteName, values.get(i), indexKey, i),
                                        refFormMap,
                                        naturalDatatypeMap))
                                .toList();
                    } else if (field instanceof IterableField itf) {
                        return evaluateIterableField(
                                itf, exprEval, datatypeMapper, currentRefForm, prefix, factoryResolver);
                    } else {
                        throw new UnsupportedOperationException("Unsupported field type: %s"
                                .formatted(field.getClass().getSimpleName()));
                    }
                })
                .toList();

        return CartesianProduct.listCartesianProduct(fieldContributions).stream()
                .map(combo -> {
                    var mergedValues = new LinkedHashMap<String, Object>();
                    var mergedRefForms = new LinkedHashMap<String, ReferenceFormulation>();
                    var mergedNaturalDatatypes = new LinkedHashMap<String, IRI>();
                    combo.forEach(ev -> {
                        mergedValues.putAll(ev.values());
                        mergedRefForms.putAll(ev.referenceFormulations());
                        mergedNaturalDatatypes.putAll(ev.naturalDatatypes());
                    });
                    return new EvaluatedValues(mergedValues, mergedRefForms, mergedNaturalDatatypes);
                })
                .toList();
    }

    private Map<String, IRI> resolveNaturalDatatypes(
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

    private List<EvaluatedValues> evaluateIterableField(
            IterableField field,
            ExpressionEvaluation exprEval,
            DatatypeMapper datatypeMapper,
            ReferenceFormulation currentRefForm,
            String prefix,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver) {

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
            nestedRefForm = currentRefForm;
        }

        var nestedFactory = factoryResolver.apply(nestedRefForm);

        var subPrefix = prefix + field.getFieldName() + ".";

        var iterableIndexKey = prefix + field.getFieldName() + INDEX_KEY_SUFFIX;

        var effectiveNestedRefForm = nestedRefForm;
        return IntStream.range(0, subRecords.size())
                .boxed()
                .flatMap(i -> {
                    var subExprEval = nestedFactory.apply(subRecords.get(i));
                    // For nested iterables, the datatype mapper is inherited from the parent
                    // evaluation context. Each sub-record uses the same mapper since the source
                    // format determines natural datatypes, not individual record content.
                    return evaluateFields(
                                    nestedFields,
                                    subExprEval,
                                    datatypeMapper,
                                    effectiveNestedRefForm,
                                    subPrefix,
                                    factoryResolver)
                            .stream()
                            .map(ev -> {
                                var mergedValues = new LinkedHashMap<>(ev.values());
                                mergedValues.put(iterableIndexKey, i);
                                var mergedNaturalDatatypes = new LinkedHashMap<>(ev.naturalDatatypes());
                                mergedNaturalDatatypes.put(iterableIndexKey, XSD.INTEGER);
                                return new EvaluatedValues(
                                        mergedValues, ev.referenceFormulations(), mergedNaturalDatatypes);
                            });
                })
                .toList();
    }

    private Flux<EvaluatedValues> applyJoin(
            Flux<EvaluatedValues> childFlux,
            LogicalViewJoin join,
            boolean isLeftJoin,
            Function<Source, ResolvedSource<?>> sourceResolver) {

        var parentFlux = evaluate(join.getParentLogicalView(), sourceResolver, EvaluationContext.defaults());

        return parentFlux.collectList().flatMapMany(parentIterations -> {
            var conditions = join.getJoinConditions().stream()
                    .sorted(Comparator.comparing(
                            c -> c.getChildMap().getExpressionMapExpressionSet().toString()))
                    .toList();
            var joinIndex = buildJoinIndex(conditions, parentIterations);
            return childFlux.flatMapIterable(
                    child -> matchAndExtend(child, conditions, join.getFields(), isLeftJoin, joinIndex));
        });
    }

    private JoinIndex<List<Object>, ViewIteration> buildJoinIndex(
            List<Join> conditions, List<ViewIteration> parentIterations) {
        var joinIndex = new HashMapJoinIndex<List<Object>, ViewIteration>();
        for (var parentIteration : parentIterations) {
            var parentExprEval = new ViewIterationExpressionEvaluation(parentIteration);
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
            JoinIndex<List<Object>, ViewIteration> joinIndex) {

        // Build child key from EvaluatedValues — child field references point to field names in
        // child iteration values, so we use a simple lookup expression evaluation.
        ExpressionEvaluation childExprEval =
                expression -> Optional.ofNullable(child.values().get(expression));

        var childKey = evaluateJoinKey(conditions, childExprEval, false);
        if (childKey.isEmpty()) {
            return isLeftJoin ? List.of(child) : List.of();
        }

        var matchedParents = joinIndex.get(childKey);
        if (matchedParents.isEmpty()) {
            return isLeftJoin ? List.of(child) : List.of();
        }

        var sortedJoinFields = joinFields.stream()
                .sorted(Comparator.comparing(ExpressionField::getFieldName))
                .toList();

        var result = new ArrayList<EvaluatedValues>();
        for (var parentIteration : matchedParents) {
            var parentExprEval = new ViewIterationExpressionEvaluation(parentIteration);

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

                        return IntStream.range(0, values.size())
                                .mapToObj(i -> new EvaluatedValues(
                                        Map.of(fieldName, values.get(i), indexKey, i), Map.of(), joinNaturalDatatypes))
                                .toList();
                    })
                    .toList();

            // Cartesian product of join field values, merged with child values
            var combinations = CartesianProduct.listCartesianProduct(fieldContributions);
            for (var combo : combinations) {
                var mergedValues = new LinkedHashMap<>(child.values());
                var mergedRefForms = new LinkedHashMap<>(child.referenceFormulations());
                var mergedNaturalDatatypes = new LinkedHashMap<>(child.naturalDatatypes());
                combo.forEach(ev -> {
                    mergedValues.putAll(ev.values());
                    mergedRefForms.putAll(ev.referenceFormulations());
                    mergedNaturalDatatypes.putAll(ev.naturalDatatypes());
                });
                result.add(new EvaluatedValues(mergedValues, mergedRefForms, mergedNaturalDatatypes));
            }
        }

        return result;
    }

    private List<Object> evaluateExpressionMap(ExpressionMap field, ExpressionEvaluation exprEval) {
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

    private List<Object> evaluateReference(String reference, ExpressionEvaluation exprEval) {
        return exprEval.apply(reference)
                .map(ExpressionEvaluation::extractValues)
                .orElse(List.of());
    }

    private List<Object> evaluateTemplate(Template template, ExpressionEvaluation exprEval) {
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
