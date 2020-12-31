package com.taxonic.carml.engine.sourceresolver;

import com.taxonic.carml.model.FileSource;
import java.util.Optional;
import java.util.function.Function;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

public interface SourceResolver extends Function<Object, Optional<Flux<DataBuffer>>> {

  default Optional<String> unpackFileSource(Object source) {
    if (source instanceof String) { // Standard rml:source
      return Optional.of((String) source);
    } else if (source instanceof FileSource) { // Extended Carml source
      return Optional.of(((FileSource) source).getUrl());
    } else {
      return Optional.empty();
    }
  }
}
