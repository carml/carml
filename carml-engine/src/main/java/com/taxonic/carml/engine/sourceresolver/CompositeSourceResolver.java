package com.taxonic.carml.engine.sourceresolver;

import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CompositeSourceResolver implements SourceResolver {

  private final Set<SourceResolver> resolvers;

  public static CompositeSourceResolver of(SourceResolver... sourceResolvers) {
    return of(ImmutableSet.copyOf(sourceResolvers));
  }

  public static CompositeSourceResolver of(Set<SourceResolver> sourceResolvers) {
    return new CompositeSourceResolver(sourceResolvers);
  }

  @Override
  public Optional<Flux<DataBuffer>> apply(Object source) {
    return resolvers.stream()
        .map(resolver -> resolver.apply(source))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

}
