package io.carml.engine.sourceresolver;

import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

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
  public Optional<Object> apply(Object source) {
    return resolvers.stream()
        .map(resolver -> resolver.apply(source))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }

}
