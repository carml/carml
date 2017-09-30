package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import java.io.InputStream;
import java.util.function.Supplier;

final class TriplesMapperComponents<T> {
	LogicalSourceResolver<T> logicalSourceResolver;
	private final InputStream source;
	private final String iteratorExpression;

	public TriplesMapperComponents(LogicalSourceResolver<T> logicalSourceResolver, InputStream source, String iteratorExpression) {
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
