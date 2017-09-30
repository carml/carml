package com.taxonic.carml.logical_source_resolver;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.util.IoUtils;

import java.util.Collections;
import java.util.Optional;

public class JsonPathResolver implements LogicalSourceResolver<Object> {

	private static Configuration JSONPATH_CONF = Configuration.builder()
			.options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();

	public SourceIterator<Object> getSourceIterator() {
		return (source, iteratorExpression) -> {
			String s = IoUtils.readAndResetInputStream(source);
			Object data = JsonPath.using(JSONPATH_CONF).parse(s).read(iteratorExpression);

			boolean isIterable = Iterable.class.isAssignableFrom(data.getClass());
			return isIterable
					? (Iterable<Object>) data
					: Collections.singleton(data);
		};
	}

	public ExpressionEvaluatorFactory<Object> getExpressionEvaluatorFactory() {
		return object -> expression -> Optional.ofNullable(
				JsonPath.using(JSONPATH_CONF).parse(object).read(expression));
	}
}
