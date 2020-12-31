package com.taxonic.carml.engine.sourceresolver;

import com.taxonic.carml.util.ReactorUtil;
import java.io.InputStream;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ClassPathResolver implements SourceResolver {

  private final String basePath;

  public static ClassPathResolver of(String basePath) {
    return new ClassPathResolver(basePath);
  }

  @Override
  public Optional<Flux<DataBuffer>> apply(Object source) {
    return unpackFileSource(source).map(relativePath -> {
      String sourceName = String.format("%s/%s", basePath, relativePath);

      InputStream inputStream = ClassPathResolver.class.getClassLoader()
          .getResourceAsStream(sourceName);

      if (inputStream == null) {
        throw new SourceResolverException(String.format("Could not resolve source %s", sourceName));
      }

      return ReactorUtil.fluxInputStream(inputStream);
    });
  }
}
