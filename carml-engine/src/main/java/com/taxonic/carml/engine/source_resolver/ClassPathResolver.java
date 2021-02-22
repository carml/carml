package com.taxonic.carml.engine.source_resolver;

import com.taxonic.carml.engine.LogicalSourceManager;
import com.taxonic.carml.engine.RmlMapper;

import java.util.Optional;

import static com.taxonic.carml.engine.source_resolver.SourceResolverUtils.unpackFileSource;

public class ClassPathResolver implements SourceResolver {

    private LogicalSourceManager sourceManager;
    private final String basePath;

    public ClassPathResolver(String basePath) {
        this.basePath = basePath;
    }

    @Override
    public void setSourceManager(LogicalSourceManager sourceManager) {
        this.sourceManager = sourceManager;
    }

    @Override
    public Optional<String> apply(Object o) {

        return unpackFileSource(o).map(f -> {
            String sourceName = basePath + "/" + f;

            // Cache source if not already done.
            if (!sourceManager.hasSource(sourceName)) {
                sourceManager.addSource(sourceName,
                    RmlMapper.class.getClassLoader()
                        .getResourceAsStream(sourceName));
            }

            return sourceManager.getSource(sourceName);
        });

    }
}
