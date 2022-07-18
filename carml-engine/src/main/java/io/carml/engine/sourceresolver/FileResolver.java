package io.carml.engine.sourceresolver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FileResolver implements SourceResolver {

  private final Path basePath;

  public static FileResolver of(Path basePath) {
    return new FileResolver(basePath);
  }

  @Override
  public Optional<Object> apply(Object source) {
    return unpackFileSource(source).map(relativePath -> {
      Path path = basePath.resolve(relativePath);
      if (Files.exists(path)) {
        try {
          return Files.newInputStream(path);
        } catch (IOException e) {
          throw new SourceResolverException(String.format("Could not resolve file path %s", path));
        }
      }

      return null;
    });
  }
}
