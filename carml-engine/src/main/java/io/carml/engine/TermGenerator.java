package io.carml.engine;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.Set;
import java.util.function.BiFunction;

public interface TermGenerator<T> extends BiFunction<ExpressionEvaluation, DatatypeMapper, Set<MappedValue<T>>> {}
