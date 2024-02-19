package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.FileSource;
import java.util.Optional;
import java.util.function.Function;

public interface SourceResolver extends Function<Object, Optional<Object>> {

  boolean supportsSource(Object sourceObject);

  default Optional<String> unpackFileSource(Object source) {
    if (source instanceof String stringSource) { // Standard rml:source
      return Optional.of(stringSource);
    } else if (source instanceof FileSource fileSource) { // Extended Carml source
      return Optional.of(fileSource.getUrl());
    } else {
      return Optional.empty();
    }
  }
}
