package io.carml.logicalview;

import static java.util.stream.Collectors.joining;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.AbstractLogicalSource;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    private final Set<MatchingLogicalSourceResolverFactory> resolverFactories;

    private record EvaluatedValues(
            Map<String, Object> values, Map<String, ReferenceFormulation> referenceFormulations) {}

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
        var index = new AtomicInteger(0);

        Flux<ExpressionEvaluation> exprEvalFlux;
        if (viewOn instanceof LogicalSource logicalSource) {
            var rootSource = logicalSource.getSource();
            var resolver = (LogicalSourceResolver<Object>) findResolver(logicalSource, rootSource);
            var expressionEvaluationFactory = resolver.getExpressionEvaluationFactory();
            factoryCache.put(rootRefForm, expressionEvaluationFactory);

            var resolvedSource = sourceResolver.apply(rootSource);
            exprEvalFlux = resolver.getLogicalSourceRecords(Set.of(logicalSource))
                    .apply(resolvedSource)
                    .map(rec -> expressionEvaluationFactory.apply(rec.getSourceRecord()));
        } else {
            exprEvalFlux = evaluateView(
                            (LogicalView) viewOn, sourceResolver, context, rootRefForm, factoryCache, factoryResolver)
                    .map(ViewIterationExpressionEvaluation::new);
        }

        Flux<ViewIteration> iterations = exprEvalFlux.flatMapIterable(exprEval -> {
            var evaluatedList = evaluateFields(fields, exprEval, rootRefForm, "", factoryResolver);
            return evaluatedList.stream()
                    .map(evaluated -> (ViewIteration) new DefaultViewIteration(
                            index.getAndIncrement(), evaluated.values(), evaluated.referenceFormulations()))
                    .toList();
        });

        return context.getLimit().map(iterations::take).orElse(iterations);
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
            ReferenceFormulation currentRefForm,
            String prefix,
            Function<ReferenceFormulation, LogicalSourceResolver.ExpressionEvaluationFactory<Object>> factoryResolver) {

        var fieldContributions = fields.stream()
                .sorted(Comparator.comparing(Field::getFieldName))
                .map(field -> {
                    if (field instanceof ExpressionField ef) {
                        var values = evaluateExpressionField(ef, exprEval);
                        var absoluteName = prefix + ef.getFieldName();
                        var refFormMap = currentRefForm != null
                                ? Map.<String, ReferenceFormulation>of(absoluteName, currentRefForm)
                                : Map.<String, ReferenceFormulation>of();
                        return values.stream()
                                .map(v -> new EvaluatedValues(Map.of(absoluteName, v), refFormMap))
                                .toList();
                    } else if (field instanceof IterableField itf) {
                        return evaluateIterableField(itf, exprEval, currentRefForm, prefix, factoryResolver);
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
                    combo.forEach(ev -> {
                        mergedValues.putAll(ev.values());
                        mergedRefForms.putAll(ev.referenceFormulations());
                    });
                    return new EvaluatedValues(mergedValues, mergedRefForms);
                })
                .toList();
    }

    private List<EvaluatedValues> evaluateIterableField(
            IterableField field,
            ExpressionEvaluation exprEval,
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

        var effectiveNestedRefForm = nestedRefForm;
        return subRecords.stream()
                .flatMap(subRecord -> {
                    var subExprEval = nestedFactory.apply(subRecord);
                    return evaluateFields(nestedFields, subExprEval, effectiveNestedRefForm, subPrefix, factoryResolver)
                            .stream();
                })
                .toList();
    }

    private List<Object> evaluateExpressionField(ExpressionField field, ExpressionEvaluation exprEval) {
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
