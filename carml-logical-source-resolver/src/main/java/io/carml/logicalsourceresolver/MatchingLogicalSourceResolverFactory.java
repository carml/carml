package io.carml.logicalsourceresolver;

import io.carml.model.LogicalSource;
import java.util.Optional;
import java.util.function.Function;

public interface MatchingLogicalSourceResolverFactory
        extends Function<LogicalSource, Optional<MatchedLogicalSourceResolverFactory>> {

    String getResolverName();
}
