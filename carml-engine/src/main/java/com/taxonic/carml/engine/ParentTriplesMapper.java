package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ParentTriplesMapper<T> {

	private static final Logger LOG = LoggerFactory.getLogger(ParentTriplesMapper.class);

	private TermGenerator<Resource> subjectGenerator;

	private Supplier<Iterable<T>> getIterator;
	private LogicalSourceResolver.ExpressionEvaluatorFactory<T> expressionEvaluatorFactory;

	ParentTriplesMapper(
		TermGenerator<Resource> subjectGenerator,
		TriplesMapperComponents<T> trMapperComponents
	) {
		this(
			subjectGenerator,
			trMapperComponents.getIterator(),
			trMapperComponents.getExpressionEvaluatorFactory()
		);
	}

	ParentTriplesMapper(
		TermGenerator<Resource> subjectGenerator,
		Supplier<Iterable<T>> getIterator,
		LogicalSourceResolver.ExpressionEvaluatorFactory<T> expressionEvaluatorFactory
	) {
		this.subjectGenerator = subjectGenerator;
		this.getIterator = getIterator;
		this.expressionEvaluatorFactory = expressionEvaluatorFactory;
	}

	Set<Resource> map(Set<Pair<String, Object>> joinValues) {

		if (joinValues.isEmpty()) {
			return ImmutableSet.of();
		}

		Set<Resource> results = new LinkedHashSet<>();
		getIterator.get().forEach(e ->
			map(e, joinValues)
				.forEach(results::add));
		return results;
	}

	private List<Resource> map(T entry, Set<Pair<String, Object>> joinValues) {
		EvaluateExpression evaluate =
				expressionEvaluatorFactory.apply(entry);

		boolean joinsValid = joinValues.stream()
				.allMatch(j -> isValidJoin(entry, j));

		if (joinsValid) {
			LOG.trace("Valid join found for entry with join {}", joinValues);
			return subjectGenerator.apply(evaluate);
		}

		return ImmutableList.of();
	}

	private boolean isValidJoin(T entry, Pair<String, Object> joinValue) {
		String parentExpression = joinValue.getLeft();
		Set<String> children = extractChildren(joinValue.getRight());

		EvaluateExpression evaluate =
				expressionEvaluatorFactory.apply(entry);
		Optional<Object> parentValue = evaluate.apply(parentExpression);
		LOG.trace("with result: {}", parentValue.map(v -> v).orElse("null"));
		return parentValue.map(v -> {
			if (v instanceof Collection<?>) {
				throw new RuntimeException(
						String.format("Parent expression [%s] in join condition leads to multiple values. "
								+ "This is not supported.", parentExpression));
			} else {
				// If one of the child values matches, the join is valid.
				return children.contains(String.valueOf(v));
			}
		}).orElse(false);
	}

	private Set<String> extractChildren(Object children) {
		if (children instanceof Collection<?>) {
			return ((Collection<?>) children).stream()
					.map(o -> (String) o)
					.collect(ImmutableCollectors.toImmutableSet());
		} else {
			return ImmutableSet.of(String.valueOf(children));
		}
	}
}
