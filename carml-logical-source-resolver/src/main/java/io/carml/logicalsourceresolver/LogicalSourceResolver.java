package io.carml.logicalsourceresolver;

import io.carml.model.LogicalSource;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

public interface LogicalSourceResolver<R> {

    Function<ResolvedSource<?>, Flux<LogicalSourceRecord<R>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSources);

    LogicalSourceResolver.ExpressionEvaluationFactory<R> getExpressionEvaluationFactory();

    // TODO javadoc
    /**
     * Will be used to derive datatype, if possible, and if not explicitly specified in mapping.
     *
     * @return an optional datatype mapper factory
     */
    Optional<DatatypeMapperFactory<R>> getDatatypeMapperFactory();

    interface ExpressionEvaluationFactory<R> extends Function<R, ExpressionEvaluation> {}

    interface DatatypeMapperFactory<R> extends Function<R, DatatypeMapper> {}

    default void logEvaluateExpression(String expression, Logger logger) {
        logger.trace("Evaluating expression: {}", expression);
    }

    DatatypeMapperFactory<?> DEFAULT_DATATYPE_MAPPER_FACTORY = r -> value -> Optional.empty();
}
