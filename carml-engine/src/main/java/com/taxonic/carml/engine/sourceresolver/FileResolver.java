package com.taxonic.carml.engine.sourceresolver;

import com.taxonic.carml.util.ReactiveInputStreams;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FileResolver implements SourceResolver {

  private final Path basePath;

  public static FileResolver of(Path basePath) {
    return new FileResolver(basePath);
  }

  @Override
  public Optional<Flux<DataBuffer>> apply(Object source) {
    return unpackFileSource(source).map(relativePath -> {
      Path path = basePath.resolve(relativePath);
      try {
        return ReactiveInputStreams.fluxInputStream(Files.newInputStream(path));
      } catch (IOException e) {
        throw new SourceResolverException(String.format("Could not resolve file path %s", path));
      }
    });
  }
}
