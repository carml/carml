package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.model.LogicalSource;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TriplesMapperComponents<T> {

	private static final Logger LOG = LoggerFactory.getLogger(TriplesMapperComponents.class);

	String name;
	LogicalSourceResolver<T> logicalSourceResolver;
	private final LogicalSource logicalSource;
	private final Function<Object, String> sourceResolver;

	public TriplesMapperComponents(String name, LogicalSourceResolver<T> logicalSourceResolver,
			LogicalSource logicalSource, Function<Object, String> sourceResolver) {
		this.name = name;
		this.logicalSourceResolver = logicalSourceResolver;
		this.logicalSource = logicalSource;
		this.sourceResolver = sourceResolver;
	}

	String getName() {
		return name;
	}

	Supplier<Iterable<T>> getIterator() {
		LOG.trace("Retrieving iterable from source {}" + (
				logicalSource.getIterator() != null ? " with iterator expression: {}" : ""),
				logicalSource.getSource(), logicalSource.getIterator());
		return logicalSourceResolver.bindSource(logicalSource, sourceResolver);
	}

	LogicalSourceResolver.ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory() {
		return logicalSourceResolver.getExpressionEvaluatorFactory();
	}
}
