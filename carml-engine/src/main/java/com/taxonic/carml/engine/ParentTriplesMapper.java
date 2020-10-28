package com.taxonic.carml.engine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.collections4.SetUtils;
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
				.allMatch(j -> isValidJoin(evaluate, j));

		if (joinsValid) {
			LOG.trace("Valid join found for entry with join {}", joinValues);
			return subjectGenerator.apply(evaluate);
		}

		return ImmutableList.of();
	}

	private boolean isValidJoin(EvaluateExpression evaluate, Pair<String, Object> joinValue) {
		LOG.trace("Determining validity of join {}", joinValue);
		String parentExpression = joinValue.getLeft();

		Object childItems = joinValue.getRight();
		LOG.trace("Extracting join's children {}", childItems);
		Set<String> children = extractValues(childItems);

		Optional<Object> parentValue = evaluate.apply(parentExpression);
		if (LOG.isTraceEnabled()) {
			LOG.trace("with result: {}", parentValue.orElse("null"));
		}

		return parentValue.map(v -> {
			if (v instanceof Collection<?>) {
				// if the intersection of parent and child values is non-empty, the join is valid
				Set<String> parentValues = extractValues(v);
				return !SetUtils.intersection(parentValues, children).isEmpty();
			} else {
				// If one of the child values matches, the join is valid.
				return children.contains(String.valueOf(v));
			}
			// using only the above if-case would be logically equivalent here,
			// but we keep the if/else as a performance optimization
		}).orElse(false);
	}

	private Set<String> extractValues(Object items) {
		if (items instanceof Collection<?>) {
			return ((Collection<?>) items).stream()
					.filter(Objects::nonNull)
					.map(Object::toString)
					.collect(ImmutableCollectors.toImmutableSet());
		} else {
			return items == null ? ImmutableSet.of() : ImmutableSet.of(items.toString());
		}
	}
}
