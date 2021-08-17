package com.taxonic.carml.logicalsourceresolver;

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.model.LogicalSource;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

public interface LogicalSourceResolver<E> {

  SourceFlux<E> getSourceFlux();

  LogicalSourceResolver.ExpressionEvaluationFactory<E> getExpressionEvaluationFactory();

  interface SourceFlux<E> extends BiFunction<Object, LogicalSource, Flux<E>> {
  }

  interface ExpressionEvaluationFactory<E> extends Function<E, ExpressionEvaluation> {
  }

  default void logEvaluateExpression(String expression, Logger logger) {
    logger.trace("Evaluating expression: {}", expression);
  }
}
