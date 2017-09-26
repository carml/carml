package com.taxonic.carml.resolvers;

import com.taxonic.carml.engine.EvaluateExpression;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class LogicalSourceResolver<SourceType> {
	public abstract SourceIterator<SourceType> getSourceIterator();
	public abstract ExpressionEvaluatorFactory<SourceType> getExpressionEvaluatorFactory();

	public Supplier<Iterable<SourceType>> bindSource(Object source, String iteratorExpression) {
		return () -> getSourceIterator().apply(source, iteratorExpression);
	}

	public interface SourceIterator<SourceType> extends BiFunction<Object, String, Iterable<SourceType>> {

	}

	public interface ExpressionEvaluatorFactory<SourceType> extends Function<SourceType, EvaluateExpression> { }

}