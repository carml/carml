package com.taxonic.carml.engine.source_resolver;

import com.taxonic.carml.engine.LogicalSourceManager;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class CompositeSourceResolver implements Function<Object, String> {

    private final Set<SourceResolver> resolvers;

    public CompositeSourceResolver(Set<SourceResolver> resolvers) {
        this.resolvers = resolvers;
    }

    @Override
    public String apply(Object source) {
        return
            resolvers.stream()
                .map(r -> r.apply(source))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElseThrow(() ->
                    new RuntimeException(String.format("could not resolve source [%s]", source)));
    }

    public void setSourceManager(LogicalSourceManager sourceManager) {
        resolvers.forEach(r -> r.setSourceManager(sourceManager));
    }
}
