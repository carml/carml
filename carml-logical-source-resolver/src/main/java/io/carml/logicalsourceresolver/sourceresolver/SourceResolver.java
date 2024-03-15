package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.FileSource;
import io.carml.model.RelativePathSource;
import java.util.Optional;
import java.util.function.Function;

public interface SourceResolver extends Function<Object, Optional<Object>> {

  boolean supportsSource(Object sourceObject);

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
