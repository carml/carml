package io.carml.logicalsourceresolver;

import io.carml.model.LogicalSource;
import io.carml.model.Source;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

public interface LogicalSourceResolver<R> {

    Function<ResolvedSource<?>, Flux<LogicalSourceRecord<R>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSources);

    /**
     * Returns a function that resolves logical source records, with access to pre-collected
     * reference expressions per logical source. Resolvers that need expression metadata
     * (e.g. SQL column projection, XPath parent-axis detection) should override this method.
     *
     * <p>The default implementation ignores the expressions and delegates to
     * {@link #getLogicalSourceRecords(Set)}.
     */
    default Function<ResolvedSource<?>, Flux<LogicalSourceRecord<R>>> getLogicalSourceRecords(
            Set<LogicalSource> logicalSources, Map<LogicalSource, Set<String>> expressionsPerLogicalSource) {
        return getLogicalSourceRecords(logicalSources);
    }

    LogicalSourceResolver.ExpressionEvaluationFactory<R> getExpressionEvaluationFactory();

    // TODO javadoc
    /**
     * Will be used to derive datatype, if possible, and if not explicitly specified in mapping.
     *
     * @return an optional datatype mapper factory
     */
    Optional<DatatypeMapperFactory<R>> getDatatypeMapperFactory();

    /**
     * Configures this resolver for the given logical source. Called by the evaluator after resolver
     * instantiation to allow format-specific initialization (e.g., namespace prefix registration for
     * XPath). The default implementation is a no-op.
     *
     * @param logicalSource the logical source to configure for
     */
    default void configure(LogicalSource logicalSource) {
        // no-op by default
    }

    /**
     * Returns a parser that converts inline text content into records of this resolver's native type.
     * Used when an {@link io.carml.model.IterableField} changes reference formulations, requiring
     * the parent field's text value to be parsed into the target format's native records.
     *
     * @return an optional inline record parser
     */
    default Optional<Function<String, List<R>>> getInlineRecordParser() {
        return Optional.empty();
    }

    interface LogicalSourceResolverFactory<R> extends Function<Source, LogicalSourceResolver<R>> {}

    interface ExpressionEvaluationFactory<R> extends Function<R, ExpressionEvaluation> {}

    interface DatatypeMapperFactory<R> extends Function<R, DatatypeMapper> {}

    default void logEvaluateExpression(String expression, Logger logger) {
        logger.trace("Evaluating expression: {}", expression);
    }

    DatatypeMapperFactory<?> DEFAULT_DATATYPE_MAPPER_FACTORY = r -> value -> Optional.empty();
}
