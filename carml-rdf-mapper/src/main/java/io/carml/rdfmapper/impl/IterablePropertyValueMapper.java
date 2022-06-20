package io.carml.rdfmapper.impl;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

@SuppressWarnings("java:S1135")
class IterablePropertyValueMapper implements PropertyValueMapper {

  private ValueTransformer valueTransformer;

  private Supplier<ImmutableCollection.Builder<Object>> createCollectionBuilder;

  public IterablePropertyValueMapper(ValueTransformer valueTransformer,
      Supplier<ImmutableCollection.Builder<Object>> createCollectionBuilder) {
    this.valueTransformer = valueTransformer;
    this.createCollectionBuilder = createCollectionBuilder;
  }

  @Override
  public Optional<Object> map(Model model, Resource resource, Object instance, List<Value> values) {

    ImmutableCollection.Builder<Object> builder = createCollectionBuilder.get();

    values.stream()
        .map(v -> valueTransformer.transform(model, v))
        .forEach(builder::add);

    return Optional.of(builder.build());

  }

  public static IterablePropertyValueMapper createForIterableType(ValueTransformer valueTransformer,
      Class<?> iterableType) {
    requireNonNull(iterableType);

    Supplier<ImmutableCollection.Builder<Object>> createCollectionBuilder =
        createCollectionBuilderFactory(iterableType);

    return new IterablePropertyValueMapper(valueTransformer, createCollectionBuilder);
  }

  private static Supplier<ImmutableCollection.Builder<Object>> createCollectionBuilderFactory(Class<?> iterableType) {

    // TODO Map<Class<?>, Supplier<Collection<Object>>>

    if (iterableType.equals(Set.class)) {
      return ImmutableSet::builder;
    } else if (iterableType.equals(List.class)) {
      return ImmutableList::builder;
    }

    throw new CarmlMapperException(
        String.format("don't know how to create a factory for collection type [%s]", iterableType.getCanonicalName()));
  }

}
