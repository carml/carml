package io.carml.logicalsourceresolver.sourceresolver;

import io.carml.model.FileSource;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ClassPathResolver implements SourceResolver {

  private final String basePath;

  private final Class<?> loadingClass;

  public static ClassPathResolver of(String basePath) {
    return of(basePath, null);
  }

  public static ClassPathResolver of(Class<?> loadingClass) {
    return of("", loadingClass);
  }

  public static ClassPathResolver of(String basePath, Class<?> loadingClass) {
    return new ClassPathResolver(basePath, loadingClass);
  }

  @Override
  public Optional<Object> apply(Object source) {
    return unpackFileSource(source).map(relativePath -> {
      String sourceName = basePath.equals("") ? relativePath : String.format("%s/%s", basePath, relativePath);

      return loadingClass == null ? ClassPathResolver.class.getClassLoader()
          .getResourceAsStream(sourceName) : loadingClass.getResourceAsStream(sourceName);
    });
  }

  @Override
  public boolean supportsSource(Object sourceObject) {
    return sourceObject instanceof String || sourceObject instanceof FileSource;
  }
}
