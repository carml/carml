package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.FileSource;
import io.carml.model.RelativePathSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FileResolver implements SourceResolver {

  private final Path basePath;

  public static FileResolver of() {
    return of(null);
  }

  public static FileResolver of(Path basePath) {
    return new FileResolver(basePath);
  }

  @Override
  public Optional<Object> apply(Object source) {
    return unpackFileSource(source).map(pathToResolve -> {
      Path path;

      if (basePath == null) {
        path = Paths.get(pathToResolve);
      } else {
        var relativePath = pathToResolve.startsWith("/") ? pathToResolve.replaceFirst("/", "") : pathToResolve;
        path = basePath.resolve(relativePath);
      }

      if (Files.exists(path)) {
        try {
          return Files.newInputStream(path);
        } catch (IOException e) {
          throw new SourceResolverException(String.format("Could not resolve file path %s", path));
        }
      }

      // TODO: give a proper error message when file path does not exist.

      return null;
    });
  }

  @Override
  public boolean supportsSource(Object sourceObject) {
    return sourceObject instanceof String || sourceObject instanceof FileSource
        || sourceObject instanceof RelativePathSource;
  }
}
