package io.carml.logicalsourceresolver;

import io.carml.model.LogicalSource;
import java.util.Optional;
import java.util.function.Function;

public interface MatchingLogicalSourceResolverSupplier
        extends Function<LogicalSource, Optional<MatchedLogicalSourceResolverSupplier>> {

    String getResolverName();
}
