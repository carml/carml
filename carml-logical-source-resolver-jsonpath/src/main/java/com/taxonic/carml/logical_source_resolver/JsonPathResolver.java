package com.taxonic.carml.logical_source_resolver;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.taxonic.carml.engine.EvaluateExpression;
import com.taxonic.carml.model.ContextEntry;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class JsonPathResolver implements LogicalSourceResolver<Object> {

	private static final Configuration JSONPATH_CONF =
			Configuration.builder()
			.options(Option.DEFAULT_PATH_LEAF_TO_NULL)
			.options(Option.SUPPRESS_EXCEPTIONS)
			.build();

	@Override
	public SourceIterator<Object> getSourceIterator() {
		return (source, logicalSource) -> {
			Object items = JsonPath.using(JSONPATH_CONF)
					.parse(source)
					.read(logicalSource.getIterator());
			return wrapItems(items);
		};
	}

	private static Iterable<Object> wrapItems(Object items) {
		if (items == null) {
			return ImmutableSet.of();
		}

		boolean isIterable = Iterable.class.isAssignableFrom(items.getClass());
		return isIterable
			? Iterables.unmodifiableIterable((Iterable<?>) items)
			: ImmutableSet.of(items);
	}

	@Override
	public ExpressionEvaluatorFactory<Object> getExpressionEvaluatorFactory() {
		// TODO reuse result of parse() across calls?
		return object -> expression -> Optional.ofNullable(
				JsonPath.using(JSONPATH_CONF).parse(object).read(expression));
	}

	@Override
	public GetIterableFromContext<Object> createGetIterableFromContext(String iterator) {
		return e -> {
			Object items = e.apply(iterator).orElse(null);
			return wrapItems(items);
		};
	}

	@Override
	public CreateContextEvaluate getCreateContextEvaluate() {
		ExpressionEvaluatorFactory<Object> f = getExpressionEvaluatorFactory();
		return (entries, evaluate) -> {
			Map<String, Object> c = createContext(entries, evaluate);
			return f.apply(c);
		};
	}

	private Map<String, Object> createContext(Set<ContextEntry> entries, EvaluateExpression evaluate) {
		return entries.stream()
			.map(e -> Pair.of(e.getKey(), evaluate.apply(e.getValueReference())))
			.filter(e -> e.getRight().isPresent())
			.collect(Collectors.toMap(
				Pair::getLeft,
				e -> e.getRight().get()
			));
	}
}
