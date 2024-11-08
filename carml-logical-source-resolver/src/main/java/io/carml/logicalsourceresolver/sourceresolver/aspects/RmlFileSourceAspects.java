package io.carml.logicalsourceresolver.sourceresolver.aspects;

import com.google.auto.service.AutoService;
import io.carml.model.DcatDistribution;
import io.carml.model.RelativePathSource;
import io.carml.model.Source;
import java.util.Optional;
import java.util.function.Function;
import org.eclipse.rdf4j.model.IRI;

@AutoService(FileSourceAspects.class)
public class RmlFileSourceAspects extends AbstractFileSourceAspects {

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public Optional<Function<Source, Optional<IRI>>> getCompression() {
        return Optional.of(source -> Optional.ofNullable(source.getCompression()));
    }

    @Override
    public boolean supportsSource(Source source) {
        return source instanceof DcatDistribution || source instanceof RelativePathSource;
    }
}
