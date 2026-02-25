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
 */
@AutoService(LogicalViewEvaluatorFactory.class)
public class DefaultLogicalViewEvaluatorFactory implements LogicalViewEvaluatorFactory {

    private final Set<MatchingLogicalSourceResolverFactory> resolverFactories;

    public DefaultLogicalViewEvaluatorFactory() {
        this.resolverFactories = ServiceLoader.load(MatchingLogicalSourceResolverFactory.class).stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Optional<MatchedLogicalViewEvaluator> match(LogicalView view) {
        var matchScore = MatchScore.builder().weakMatch().build();
        var evaluator = new DefaultLogicalViewEvaluator(resolverFactories);
        return Optional.of(MatchedLogicalViewEvaluator.of(matchScore, evaluator));
    }
}
