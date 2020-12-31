package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public interface ExpressionEvaluation extends Function<String, Optional<Object>> {

  // TODO explain usage in javadoc
  static List<String> extractValues(Object items) {
    if (items instanceof Collection<?>) {
      return ((Collection<?>) items).stream()
          .filter(Objects::nonNull)
          .map(Object::toString)
          .collect(ImmutableList.toImmutableList());
    } else {
      return items == null ? ImmutableList.of() : ImmutableList.of(items.toString());
    }
  }

}
