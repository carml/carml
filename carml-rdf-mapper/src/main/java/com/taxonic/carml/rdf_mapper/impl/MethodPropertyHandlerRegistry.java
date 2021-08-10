package com.taxonic.carml.rdf_mapper.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.rdf_mapper.PropertyHandler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public class MethodPropertyHandlerRegistry {

  private final Method method;

  private final boolean isIterable;

  private final Set<PropertyHandler> handlers;

  private MethodPropertyHandlerRegistry(Method method, boolean isIterable, Set<PropertyHandler> handlers) {
    this.method = method;
    this.isIterable = isIterable;
    this.handlers = handlers;
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {

    private Method method;

    private Boolean isIterable;

    private final ImmutableSet.Builder<PropertyHandler> handlersBuilder = new ImmutableSet.Builder<>();

    void method(Method method) {
      this.method = method;
    }

    void isIterable(Boolean isIterable) {
      this.isIterable = isIterable;
    }

    void addHandler(PropertyHandler handler) {
      handlersBuilder.add(handler);
    }

    boolean isBuildable() {
      return isIterable != null;
    }

    MethodPropertyHandlerRegistry build() {
      if (method == null) {
        throw new IllegalStateException("method cannot be null");
      }
      if (isIterable == null) {
        throw new IllegalStateException("isIterable cannot be null");
      }
      return new MethodPropertyHandlerRegistry(method, isIterable, handlersBuilder.build());
    }
  }

  Method getMethod() {
    return method;
  }

  Set<PropertyHandler> getHandlers() {
    return handlers;
  }

  Stream<Optional<PropertyHandler>> getEffectiveHandlers(Model model, Resource resource) {
    List<PropertyHandler> effectiveHandlers = handlers.stream()
        .filter(h -> h.hasEffect(model, resource))
        .collect(Collectors.toList());

    // Only apply one effective property handler for non-iterable property
    if (!isIterable) {
      return Stream.of(effectiveHandlers.stream()
          .findFirst());
    }

    // for iterable property
    if (!effectiveHandlers.isEmpty()) {
      // if at least one effective handler, return all
      return effectiveHandlers.stream()
          .map(Optional::ofNullable);
    } else {
      // if no effective handlers, return the first ineffective one for initialization of iterable type
      return Stream.of(handlers.stream()
          .findFirst());
    }
  }

}
