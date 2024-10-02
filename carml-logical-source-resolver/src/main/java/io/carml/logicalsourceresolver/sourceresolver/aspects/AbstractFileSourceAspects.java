package io.carml.logicalsourceresolver.sourceresolver.aspects;

import io.carml.logicalsourceresolver.sourceresolver.PathRelativeTo;
import io.carml.model.Source;
import java.net.URL;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;
import org.eclipse.rdf4j.model.IRI;

public abstract class AbstractFileSourceAspects implements FileSourceAspects {

    @Override
    public Optional<Function<Source, Optional<URL>>> getUrl() {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Source, Optional<Path>>> getBasePath() {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Source, Optional<String>>> getPathString() {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Source, Optional<PathRelativeTo>>> getPathRelativeTo() {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Source, Optional<IRI>>> getCompression() {
        return Optional.empty();
    }
}
