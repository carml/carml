package io.carml.rdfmapper.impl;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

import io.carml.rdfmapper.PropertyHandler;
import io.carml.rdfmapper.annotations.RdfProperty;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Qualifier;

/**
 * An instance of this class is used to create an instance of a custom property handler, based on an
 * annotation on a getter or setter. This class facilitates also performs some dependency injection
 * on the instantiated handler, so that custom code may have access to the current {@code Mapper}
 * instance, for example.
 *
 * @see RdfProperty
 * @see DependencyResolver
 */
public class SpecifiedPropertyHandlerFactory {

  private DependencySettersCache cache;

  public SpecifiedPropertyHandlerFactory(DependencySettersCache cache) {
    this.cache = cache;
  }

  public Optional<PropertyHandler> createPropertyHandler(RdfProperty annotation, DependencyResolver resolver) {

    if (annotation.handler()
        .equals(PropertyHandler.class)) {
      return Optional.empty();
    }

    Class<? extends PropertyHandler> handlerCls = annotation.handler();

    PropertyHandler handler = createInstance(handlerCls);

    injectDependencies(handlerCls, resolver, handler);

    return Optional.of(handler);
  }

  private <T extends PropertyHandler> T createInstance(Class<T> handlerCls) {
    try {
      return handlerCls.getConstructor()
          .newInstance();
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException exception) {
      throw new CarmlMapperException(
          String.format("could not instantiate specified PropertyHandler class [%s]", handlerCls.getCanonicalName()),
          exception);
    }
  }

  private List<Consumer<Object>> createAndCacheDependencySetters(Class<?> cls, DependencyResolver resolver) {
    List<Consumer<Object>> dependencySetters = createDependencySetters(cls, resolver);
    cache.put(cls, dependencySetters);
    return dependencySetters;
  }

  private List<Consumer<Object>> createDependencySetters(Class<?> cls, DependencyResolver resolver) {

    return stream(cls.getMethods())

        // find methods annotated with @Inject
        .filter(m -> m.getAnnotation(Inject.class) != null)

        // for each such setter, create a consumer that will take a
        // handler instance, and will resolve and set the correct
        // dependency.
        .map(m -> createDependencySetter(m, resolver, getPropertyType(m), getPropertyQualifiers(m)))

        .collect(toList());

  }

  // do dependency injection through setter methods annotated with @Inject
  private void injectDependencies(Class<?> cls, DependencyResolver resolver, Object instance) {
    cache.get(cls)
        .orElse(createAndCacheDependencySetters(cls, resolver))
        .forEach(i -> i.accept(instance));
  }

  private Type getPropertyType(Method method) {
    List<Type> parameterTypes = asList(method.getGenericParameterTypes());
    if (parameterTypes.size() != 1) {
      throw new CarmlMapperException(
          String.format("method [%s], annotated with @Inject does NOT take exactly 1 parameter; it takes %s",
              method.getName(), parameterTypes.size()));
    }

    return parameterTypes.get(0);
  }

  private boolean isQualifierInstance(Annotation annotation) {
    return annotation.annotationType()
        .getAnnotation(Qualifier.class) != null;
  }

  private List<Annotation> getPropertyQualifiers(Method method) {
    return asList(method.getAnnotations()).stream()
        .filter(this::isQualifierInstance)
        .collect(toList());
  }

  private Consumer<Object> createDependencySetter(Method method, DependencyResolver resolver, Type propertyType,
      List<Annotation> qualifiers) {
    return i -> {

      // resolve dependency through resolver, which may use the property
      // type and qualifiers, if any.
      Object propertyValue = resolver.resolve(propertyType, qualifiers);
      try {

        // invoke the setter to set the resolved value
        method.invoke(i, propertyValue);

      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException exception) {
        throw new CarmlMapperException(String.format("error invoking setter [%s]", method.getName()), exception);
      }
    };
  }

}
