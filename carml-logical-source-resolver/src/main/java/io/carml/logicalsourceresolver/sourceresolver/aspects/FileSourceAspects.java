package io.carml.logicalsourceresolver.sourceresolver.aspects;

import io.carml.logicalsourceresolver.sourceresolver.PathRelativeTo;
import io.carml.logicalsourceresolver.sourceresolver.SourceSupport;
import io.carml.model.Source;
import java.net.URL;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;
import org.eclipse.rdf4j.model.IRI;

public interface FileSourceAspects extends SourceSupport {

    int getPriority();

    Optional<Function<Source, Optional<URL>>> getUrl();

    Optional<Function<Source, Optional<Path>>> getBasePath();

    Optional<Function<Source, Optional<String>>> getPathString();

    Optional<Function<Source, Optional<PathRelativeTo>>> getPathRelativeTo();

    Optional<Function<Source, Optional<IRI>>> getCompression();
}
