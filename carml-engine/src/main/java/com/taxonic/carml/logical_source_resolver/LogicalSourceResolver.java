package com.taxonic.carml.logical_source_resolver;

import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.model.LogicalSource;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public interface LogicalSourceResolver<T> {
	SourceIterator<T> getSourceIterator();
	ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory();

	default Supplier<Iterable<T>> bindSource(LogicalSource logicalSource, Function<Object, String> sourceResolver) {
		return () -> getSourceIterator()
				.apply(sourceResolver.apply(logicalSource.getSource()), logicalSource);
	}

	interface SourceIterator<T> extends BiFunction<String, LogicalSource, Iterable<T>> {}

	interface ExpressionEvaluatorFactory<T> extends Function<T, EvaluateExpression> {}

}
