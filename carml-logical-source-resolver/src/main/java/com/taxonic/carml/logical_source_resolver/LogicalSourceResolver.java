package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.engine.Item;
import com.taxonic.carml.model.ContextEntry;
import com.taxonic.carml.model.LogicalSource;
import org.slf4j.Logger;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface LogicalSourceResolver<T> {
	SourceStream<T> getSourceStream(); // TODO rename createGetSourceStream()
	GetStreamFromContext<T> createGetStreamFromContext(String iterator);
	CreateContextEvaluate getCreateContextEvaluate();

	default Supplier<Stream<Item<T>>> bindSource(LogicalSource logicalSource, Function<Object, String> sourceResolver) {
		return () -> getSourceStream()
				.apply(sourceResolver.apply(logicalSource.getSource()), logicalSource);
	}

	interface SourceStream<T> extends BiFunction<String, LogicalSource, Stream<Item<T>>> {}

	interface ExpressionEvaluatorFactory<T> extends Function<T, EvaluateExpression> {}

	interface GetStreamFromContext<T> extends Function<EvaluateExpression, Stream<Item<T>>> {}

	interface CreateContextEvaluate extends BiFunction<Set<ContextEntry>, EvaluateExpression, EvaluateExpression> {}

	default void logEvaluateExpression(String expression, Logger logger) {
		logger.trace("Evaluating expression: {}", expression);
	}

}
