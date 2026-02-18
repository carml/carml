package io.carml.logicalsourceresolver.sourceresolver.aspects;

import com.google.auto.service.AutoService;
import io.carml.logicalsourceresolver.sourceresolver.PathRelativeTo;
import io.carml.model.FilePath;
import io.carml.model.Source;
import io.carml.vocab.Rdf.Rml;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;
import org.eclipse.rdf4j.model.Literal;

@AutoService(FileSourceAspects.class)
public class FilePathAspects extends AbstractFileSourceAspects {

    @Override
    public int getPriority() {
        return 10;
    }

    @Override
    public boolean supportsSource(Source source) {
        return source instanceof FilePath;
    }

    @Override
    public Optional<Function<Source, Optional<Path>>> getBasePath() {
        return Optional.of(source -> {
            if (source instanceof FilePath filePath) {
                var root = filePath.getRoot();
                if (root instanceof Literal literalRoot) {
                    return Optional.of(Path.of(literalRoot.stringValue()));
                }
            }

            return Optional.empty();
        });
    }

    @Override
    public Optional<Function<Source, Optional<PathRelativeTo>>> getPathRelativeTo() {
        return Optional.of(source -> {
            if (source instanceof FilePath filePath) {
                var root = filePath.getRoot();
                if (root == null || root.equals(Rml.CurrentWorkingDirectory)) {
                    return Optional.of(PathRelativeTo.WORKING_DIRECTORY);
                }
                if (root.equals(Rml.MappingDirectory)) {
                    return Optional.of(PathRelativeTo.MAPPING_DIRECTORY);
                }
            }

            return Optional.empty();
        });
    }

    @Override
    public Optional<Function<Source, Optional<String>>> getPathString() {
        return Optional.of(source -> {
            if (source instanceof FilePath filePath) {
                return Optional.of(filePath.getPath());
            }

            return Optional.empty();
        });
    }
}
