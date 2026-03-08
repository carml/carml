package io.carml.engine.rdf;

import io.carml.engine.RmlMapperException;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.logicalview.EvaluationContext;
import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.LogicalViewEvaluatorFactory;
import io.carml.logicalview.MatchedLogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalView;
import io.carml.model.Source;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PACKAGE)
class FactoryDelegatingEvaluator implements LogicalViewEvaluator {

    private final List<LogicalViewEvaluatorFactory> factories;

    @Override
    public Flux<ViewIteration> evaluate(
            LogicalView view, Function<Source, ResolvedSource<?>> sourceResolver, EvaluationContext context) {
        var matches = factories.stream()
                .map(f -> f.match(view))
                .flatMap(Optional::stream)
                .toList();
        var evaluator = MatchedLogicalViewEvaluator.select(matches)
                .orElseThrow(() -> new RmlMapperException("No evaluator matched logical view: %s".formatted(view)));
        LOG.debug("Selected evaluator {} for view {}", evaluator.getClass().getSimpleName(), view);
        return evaluator.evaluate(view, sourceResolver, context);
    }
}
