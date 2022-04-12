package com.taxonic.carml.logicalsourceresolver;

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.model.LogicalSource;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

public interface LogicalSourceResolver<R> {

  Function<ResolvedSource<?>, Flux<LogicalSourceRecord<R>>> getLogicalSourceRecords(Set<LogicalSource> logicalSources);

  LogicalSourceResolver.ExpressionEvaluationFactory<R> getExpressionEvaluationFactory();

  interface ExpressionEvaluationFactory<R> extends Function<R, ExpressionEvaluation> {
  }

  default void logEvaluateExpression(String expression, Logger logger) {
    logger.trace("Evaluating expression: {}", expression);
  }
}
