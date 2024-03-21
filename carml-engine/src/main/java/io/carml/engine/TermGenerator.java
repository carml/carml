package io.carml.engine;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import java.util.List;
import java.util.function.BiFunction;

public interface TermGenerator<T> extends BiFunction<ExpressionEvaluation, DatatypeMapper, List<MappedValue<T>>> {}
