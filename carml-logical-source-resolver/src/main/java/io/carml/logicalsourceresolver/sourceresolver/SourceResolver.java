package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.FileSource;
import io.carml.model.RelativePathSource;
import io.carml.model.Source;
import java.util.Optional;
import java.util.function.Function;

public interface SourceResolver extends Function<Source, Optional<Object>> {
    boolean supportsSource(Source source);

    default Optional<String> unpackFileSource(Object source) {
        if (source instanceof RelativePathSource relativePathSource) {
            return Optional.of(relativePathSource.getPath());
        } else if (source instanceof String stringSource) {
            return Optional.of(stringSource);
        } else if (source instanceof FileSource fileSource) {
            return Optional.of(fileSource.getUrl());
        } else {
            return Optional.empty();
        }
    }
}
