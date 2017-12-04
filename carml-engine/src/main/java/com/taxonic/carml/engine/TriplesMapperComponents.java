package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import java.util.function.Supplier;

final class TriplesMapperComponents<T> {
	LogicalSourceResolver<T> logicalSourceResolver;
	private final String source;
	private final String iteratorExpression;

	public TriplesMapperComponents(LogicalSourceResolver<T> logicalSourceResolver, String source, String iteratorExpression) {
		this.logicalSourceResolver = logicalSourceResolver;
		this.source = source;
		this.iteratorExpression = iteratorExpression;
	}

	Supplier<Iterable<T>> getIterator() {
		return logicalSourceResolver.bindSource(source, iteratorExpression);
	}

	LogicalSourceResolver.ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory() {
		return logicalSourceResolver.getExpressionEvaluatorFactory();
	}
}
