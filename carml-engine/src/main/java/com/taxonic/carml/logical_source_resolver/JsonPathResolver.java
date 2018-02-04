package com.taxonic.carml.logical_source_resolver;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.util.Optional;

public class JsonPathResolver implements LogicalSourceResolver<Object> {

	private static final Configuration JSONPATH_CONF = Configuration.builder()
			.options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();

	public SourceIterator<Object> getSourceIterator() {
		return (source, logicalSource) -> {
			Object data = JsonPath.using(JSONPATH_CONF).parse(source).read(logicalSource.getIterator());

			boolean isIterable = Iterable.class.isAssignableFrom(data.getClass());
			return isIterable
					? Iterables.unmodifiableIterable((Iterable<?>)data)
					: ImmutableSet.of(data);
		};
	}

	public ExpressionEvaluatorFactory<Object> getExpressionEvaluatorFactory() {
		return object -> expression -> Optional.ofNullable(
				JsonPath.using(JSONPATH_CONF).parse(object).read(expression));
	}
}
