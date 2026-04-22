package io.carml.logicalview;

import com.google.auto.service.AutoService;
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
 */
@AutoService(LogicalViewEvaluatorFactory.class)
public class DefaultLogicalViewEvaluatorFactory implements LogicalViewEvaluatorFactory {

    private final Set<MatchingLogicalSourceResolverFactory> resolverFactories;

    private final JoinExecutorFactory joinExecutorFactory;

    public DefaultLogicalViewEvaluatorFactory() {
        this(JoinExecutorFactory.inMemory());
    }

    public DefaultLogicalViewEvaluatorFactory(JoinExecutorFactory joinExecutorFactory) {
        this.resolverFactories = ServiceLoader.load(MatchingLogicalSourceResolverFactory.class).stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.toUnmodifiableSet());
        this.joinExecutorFactory = joinExecutorFactory;
    }

    @Override
    public Optional<MatchedLogicalViewEvaluator> match(LogicalView view) {
        var matchScore = MatchScore.builder().weakMatch().build();
        var evaluator = new DefaultLogicalViewEvaluator(resolverFactories, joinExecutorFactory);
        return Optional.of(MatchedLogicalViewEvaluator.of(matchScore, evaluator));
    }
}
