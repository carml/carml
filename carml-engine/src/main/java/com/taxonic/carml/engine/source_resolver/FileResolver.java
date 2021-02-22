package com.taxonic.carml.engine.source_resolver;

import com.taxonic.carml.engine.LogicalSourceManager;
import com.taxonic.carml.engine.RmlMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.taxonic.carml.engine.source_resolver.SourceResolverUtils.unpackFileSource;

public class FileResolver implements SourceResolver {

    private LogicalSourceManager sourceManager;
    private final Path basePath;

    public FileResolver(Path basePath) {
        this.basePath = basePath;
    }

    @Override
    public void setSourceManager(LogicalSourceManager sourceManager) {
        this.sourceManager = sourceManager;
    }

    @Override
    public Optional<String> apply(Object o) {

        return unpackFileSource(o).map(f -> {
            Path path = basePath.resolve(f);
            String sourceName = path.toString();

            // Cache source if not already done.
            if (!sourceManager.hasSource(sourceName)) {
                try {
                    sourceManager.addSource(sourceName, new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
                }
                catch (IOException e) {
                    throw new RuntimeException(String.format("could not create file source for path [%s]", path));
                }
            }

            return sourceManager.getSource(sourceName);
        });
    }
}
