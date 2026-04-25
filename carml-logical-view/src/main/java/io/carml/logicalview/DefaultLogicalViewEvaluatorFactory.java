package io.carml.logicalview;

import com.google.auto.service.AutoService;
import io.carml.functions.FunctionRegistry;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.model.LogicalView;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default {@link LogicalViewEvaluatorFactory} implementation that always matches any
 * {@link LogicalView} with a weak score, serving as the universal fallback evaluator. Discovered
 * via {@link ServiceLoader} and produces a {@link DefaultLogicalViewEvaluator} backed by
 * {@link MatchingLogicalSourceResolverFactory} instances loaded from the module path.
 *
 * <p>The factory accepts an optional {@link JoinExecutorFactory} that the produced evaluator uses
 * to materialize joins. The default is {@link JoinExecutorFactory#inMemory()} (HashMap probe);
 * applications opting into spill-to-disk pass a spillable factory (e.g. {@code
 * DuckDbJoinExecutorFactory}).
 *
 * <p>The factory also carries an {@link ExpressionMapEvaluator} used by the produced
 * evaluator to resolve {@link io.carml.model.ExpressionMap expression maps} in join conditions and
 * expression fields. Unless an evaluator is supplied explicitly, the factory constructs a
 * {@link DefaultExpressionMapEvaluator} backed by {@link FunctionRegistry#create()} — i.e. a
 * fresh registry pre-populated with the SPI-discovered function descriptors. Applications that
 * want join maps to see functions registered via {@code RdfRmlMapper.Builder#function(...)} should
 * either go through the builder (which threads the mapper's registry into the discovered default
 * factory) or construct this factory with an explicit {@link FunctionRegistry} argument.
 */
@AutoService(LogicalViewEvaluatorFactory.class)
public class DefaultLogicalViewEvaluatorFactory implements LogicalViewEvaluatorFactory {

    private final Set<MatchingLogicalSourceResolverFactory> resolverFactories;

    private final JoinExecutorFactory joinExecutorFactory;

    private final ExpressionMapEvaluator expressionMapValueEvaluator;

    public DefaultLogicalViewEvaluatorFactory() {
        this(JoinExecutorFactory.inMemory(), new DefaultExpressionMapEvaluator(FunctionRegistry.create()));
    }

    public DefaultLogicalViewEvaluatorFactory(JoinExecutorFactory joinExecutorFactory) {
        this(joinExecutorFactory, new DefaultExpressionMapEvaluator(FunctionRegistry.create()));
    }

    /**
     * Creates a factory that produces evaluators using a {@link DefaultExpressionMapEvaluator}
     * backed by the given {@link FunctionRegistry}.
     */
    public DefaultLogicalViewEvaluatorFactory(
            JoinExecutorFactory joinExecutorFactory, FunctionRegistry functionRegistry) {
        this(joinExecutorFactory, new DefaultExpressionMapEvaluator(functionRegistry));
    }

    public DefaultLogicalViewEvaluatorFactory(
            JoinExecutorFactory joinExecutorFactory, ExpressionMapEvaluator expressionMapValueEvaluator) {
        this.resolverFactories = ServiceLoader.load(MatchingLogicalSourceResolverFactory.class).stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.toUnmodifiableSet());
        this.joinExecutorFactory = joinExecutorFactory;
        this.expressionMapValueEvaluator = expressionMapValueEvaluator;
    }

    @Override
    public Optional<MatchedLogicalViewEvaluator> match(LogicalView view) {
        var matchScore = MatchScore.builder().weakMatch().build();
        var evaluator =
                new DefaultLogicalViewEvaluator(resolverFactories, joinExecutorFactory, expressionMapValueEvaluator);
        return Optional.of(MatchedLogicalViewEvaluator.of(matchScore, evaluator));
    }
}
