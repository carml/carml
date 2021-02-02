package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.ExpressionEvaluatorFactory;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.GetIterableFromContext;
import com.taxonic.carml.model.LogicalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;
import java.util.function.Supplier;

final class TriplesMapperComponents<T> {

	private static final Logger LOG = LoggerFactory.getLogger(TriplesMapperComponents.class);

	String name;
	LogicalSourceResolver<T> logicalSourceResolver;
	private final LogicalSource logicalSource;
	private final Function<Object, String> sourceResolver;
	private final Supplier<Iterable<T>> iterator;

	public TriplesMapperComponents(String name, LogicalSourceResolver<T> logicalSourceResolver,
			LogicalSource logicalSource, Function<Object, String> sourceResolver, Supplier<Iterable<T>> iterator) {
		this.name = name;
		this.logicalSourceResolver = logicalSourceResolver;
		this.logicalSource = logicalSource;
		this.sourceResolver = sourceResolver;
		this.iterator = iterator;
	}

	String getName() {
		return name;
	}

	Supplier<Iterable<T>> getIterator() {
		if (iterator != null) {
			return iterator;
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("Retrieving iterable from source {}" + (
							logicalSource.getIterator() != null ? " with iterator expression: {}" : ""),
					logicalSource.getSource(), logicalSource.getIterator());
		}
		return logicalSourceResolver.bindSource(logicalSource, sourceResolver);
	}

	ExpressionEvaluatorFactory<T> getExpressionEvaluatorFactory() {
		return logicalSourceResolver.getExpressionEvaluatorFactory();
	}

	GetIterableFromContext<T> createGetIterableFromContext(String iteratorExpression) {
		return logicalSourceResolver.createGetIterableFromContext(iteratorExpression);
	}

	LogicalSourceResolver<T> getLogicalSourceResolver() {
		return logicalSourceResolver;
	}
}
