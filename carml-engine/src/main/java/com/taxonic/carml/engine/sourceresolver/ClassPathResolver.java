package com.taxonic.carml.engine.sourceresolver;

import java.io.InputStream;
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

      InputStream inputStream = loadingClass == null ? ClassPathResolver.class.getClassLoader()
          .getResourceAsStream(sourceName) : loadingClass.getResourceAsStream(sourceName);

      if (inputStream == null) {
        throw new SourceResolverException(String.format("Could not resolve source %s", sourceName));
      }

      return inputStream;
    });
  }
}
