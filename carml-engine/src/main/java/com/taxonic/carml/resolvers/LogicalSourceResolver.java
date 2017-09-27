package com.taxonic.carml.resolvers;

import com.taxonic.carml.engine.EvaluateExpression;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public interface LogicalSourceResolver<T> {
	SourceIterator<T> getSourceIterator();
	ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory();

	default Supplier<Iterable<T>> bindSource(Object source, String iteratorExpression) {
		return () -> getSourceIterator().apply(source, iteratorExpression);
	}

	interface SourceIterator<T> extends BiFunction<Object, String, Iterable<T>> {

	}

	interface ExpressionEvaluatorFactory<T> extends Function<T, EvaluateExpression> { }

}