package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.LogicalSource;
import org.slf4j.Logger;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public interface LogicalSourceResolver<T> {
	SourceIterator<T> getSourceIterator();
	ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory();
	GetIterableFromContext<T> createGetIterableFromContext(String iterator);
	CreateContextEvaluate getCreateContextEvaluate();

	default Supplier<Iterable<T>> bindSource(LogicalSource logicalSource, Function<Object, String> sourceResolver) {
		return () -> getSourceIterator()
				.apply(sourceResolver.apply(logicalSource.getSource()), logicalSource);
	}

	interface SourceIterator<T> extends BiFunction<String, LogicalSource, Iterable<T>> {}

	interface ExpressionEvaluatorFactory<T> extends Function<T, EvaluateExpression> {}

	interface GetIterableFromContext<T> extends Function<EvaluateExpression, Iterable<T>> {}

	interface CreateContextEvaluate extends BiFunction<Set<ContextEntry>, EvaluateExpression, EvaluateExpression> {}

	default void logEvaluateExpression(String expression, Logger logger) {
		logger.trace("Evaluating expression: {}", expression);
	}

}
