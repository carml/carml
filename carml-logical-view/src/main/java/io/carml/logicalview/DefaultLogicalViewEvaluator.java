package io.carml.logicalview;

import static java.util.stream.Collectors.joining;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.LogicalSourceRecord;
import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.ExpressionField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.Source;
import io.carml.model.Template;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import io.carml.util.CartesianProduct;
import java.util.ArrayList;
import java.util.Comparator;
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
 * root-level {@link ExpressionField} entries (reference, template, constant) and builds view
 * iterations via Cartesian product expansion of multi-valued fields.
 */
@Slf4j
@AllArgsConstructor
public class DefaultLogicalViewEvaluator implements LogicalViewEvaluator {

    private final Set<MatchingLogicalSourceResolverFactory> resolverFactories;

    @Override
    @SuppressWarnings("unchecked")
    public Flux<ViewIteration> evaluate(
            LogicalView view, Function<Source, ResolvedSource<?>> sourceResolver, EvaluationContext context) {

        var logicalSource = resolveLogicalSource(view);
        var source = logicalSource.getSource();
        var resolvedSource = sourceResolver.apply(source);

        var resolver = (LogicalSourceResolver<Object>) findResolver(logicalSource, source);
        var expressionEvaluationFactory = resolver.getExpressionEvaluationFactory();

        var expressionFields = view.getFields().stream()
                .filter(ExpressionField.class::isInstance)
                .map(ExpressionField.class::cast)
                .sorted(Comparator.comparing(ExpressionField::getFieldName))
                .toList();

        var index = new AtomicInteger(0);

        Flux<ViewIteration> iterations = resolver.getLogicalSourceRecords(Set.of(logicalSource))
                .apply(resolvedSource)
                .flatMapIterable((LogicalSourceRecord<Object> rec) ->
                        evaluateRecord(rec, expressionEvaluationFactory, expressionFields, index));

        return context.getLimit().map(iterations::take).orElse(iterations);
    }

    private LogicalSource resolveLogicalSource(LogicalView view) {
        var viewOn = view.getViewOn();
        if (viewOn instanceof LogicalSource logicalSource) {
            return logicalSource;
        }
        throw new LogicalSourceResolverException("LogicalView viewOn must be a LogicalSource, but was %s"
                .formatted(viewOn.getClass().getSimpleName()));
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

    private <R> List<ViewIteration> evaluateRecord(
            LogicalSourceRecord<R> rec,
            LogicalSourceResolver.ExpressionEvaluationFactory<R> expressionEvaluationFactory,
            List<ExpressionField> expressionFields,
            AtomicInteger index) {

        var exprEval = expressionEvaluationFactory.apply(rec.getSourceRecord());

        var fieldNames = new ArrayList<String>();
        var fieldValueLists = new ArrayList<List<Object>>();

        for (var field : expressionFields) {
            var values = evaluateExpressionField(field, exprEval);
            fieldNames.add(field.getFieldName());
            fieldValueLists.add(values);
        }

        var combinations = CartesianProduct.listCartesianProduct(fieldValueLists);

        return combinations.stream()
                .map(combination -> {
                    Map<String, Object> valueMap = new LinkedHashMap<>();
                    for (int i = 0; i < fieldNames.size(); i++) {
                        valueMap.put(fieldNames.get(i), combination.get(i));
                    }
                    return (ViewIteration) new DefaultViewIteration(index.getAndIncrement(), valueMap);
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
