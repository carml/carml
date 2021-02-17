package com.taxonic.carml.logical_source_resolver;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.taxonic.carml.engine.Item;
import com.taxonic.carml.model.LogicalSource;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.taxonic.carml.logical_source_resolver.util.ContextUtils.createContextMap;

public class JsonPathResolver implements LogicalSourceResolver<Object> {

	private static final Configuration JSONPATH_CONF =
			Configuration.builder()
			.options(Option.DEFAULT_PATH_LEAF_TO_NULL)
			.options(Option.SUPPRESS_EXCEPTIONS)
			.build();

	@Override
	public SourceStream<Object> getSourceStream() {
		return (String source, LogicalSource logicalSource) -> {
			Object items = JsonPath.using(JSONPATH_CONF)
					.parse(source)
					.read(logicalSource.getIterator());
			return wrapItems(items);
		};
	}

	@SuppressWarnings("unchecked")
	private Stream<Item<Object>> wrapItems(Object items) {
		if (items == null) {
			return Stream.empty();
		}

		ExpressionEvaluatorFactory<Object> evaluatorFactory = getExpressionEvaluatorFactory();

		boolean isIterable = Iterable.class.isAssignableFrom(items.getClass());
		return (isIterable
			? StreamSupport.stream(((Iterable<Object>) items).spliterator(), false)
			: Stream.of(items))
			.map(o -> new Item<>(o, evaluatorFactory.apply(o)));
	}

	private ExpressionEvaluatorFactory<Object> getExpressionEvaluatorFactory() {
		// TODO reuse result of parse() across calls?
		return object -> expression -> Optional.ofNullable(
				JsonPath.using(JSONPATH_CONF).parse(object).read(expression));
	}

	@Override
	public GetStreamFromContext<Object> createGetStreamFromContext(String iterator) {
		return e -> {
			Object items = e.apply(iterator).orElse(null);
			return wrapItems(items);
		};
	}

	@Override
	public CreateContextEvaluate getCreateContextEvaluate() {
		ExpressionEvaluatorFactory<Object> f = getExpressionEvaluatorFactory();
		return (entries, evaluate) -> {
			Map<String, Object> c = createContextMap(entries, evaluate);
			return f.apply(c);
		};
	}

	@Override
	public CreateSimpleTypedRepresentation getCreateSimpleTypedRepresentation() {
		return v -> v;
	}
}
