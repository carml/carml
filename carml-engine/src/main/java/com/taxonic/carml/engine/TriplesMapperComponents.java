package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.LogicalSource;
import java.util.function.Function;
import java.util.function.Supplier;

final class TriplesMapperComponents<T> {
	LogicalSourceResolver<T> logicalSourceResolver;
	private final LogicalSource logicalSource;
	private final Function<Object, String> sourceResolver;

	public TriplesMapperComponents(LogicalSourceResolver<T> logicalSourceResolver, 
			LogicalSource logicalSource, Function<Object, String> sourceResolver) {
		this.logicalSourceResolver = logicalSourceResolver;
		this.logicalSource = logicalSource;
		this.sourceResolver = sourceResolver;
	}

	Supplier<Iterable<T>> getIterator() {
		return logicalSourceResolver.bindSource(logicalSource, sourceResolver);
	}

	LogicalSourceResolver.ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory() {
		return logicalSourceResolver.getExpressionEvaluatorFactory();
	}
}
