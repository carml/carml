package com.taxonic.carml.logical_source_resolver.util;

import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.model.ContextEntry;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

abstract public class ContextUtils {

    @SuppressWarnings("unchecked")
    public static <T> Map<String, T> createContextMap(Set<ContextEntry> entries, EvaluateExpression evaluate) {
        return entries.stream()
            .map(e -> new OptionalResult<>(e.getAs(), (Optional<T>) evaluate.apply(e.getReference())))
            .filter(OptionalResult::isPresent)
            .collect(Collectors.toMap(
                OptionalResult::getAs,
                OptionalResult::getResult
            ));
    }
}

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
class OptionalResult<T> {

    private final String as;
    private final Optional<T> result;

    OptionalResult(String as, Optional<T> result) {
        this.as = as;
        this.result = result;
    }

    boolean isPresent() {
        return result.isPresent();
    }

    public String getAs() {
        return as;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public T getResult() {
        return result.get();
    }
}