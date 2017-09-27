package com.taxonic.carml.engine;

import com.taxonic.carml.resolvers.LogicalSourceResolver;

import org.eclipse.rdf4j.model.Resource;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

class ParentTriplesMapper<T> {
	
	private TermGenerator<Resource> subjectGenerator;

	private Supplier<Iterable<T>> getIterator;
	private LogicalSourceResolver.ExpressionEvaluatorFactory<T> expressionEvaluatorFactory;

		ParentTriplesMapper(
			TermGenerator<Resource> subjectGenerator,

			Supplier<Iterable<T>> getIterator,
			LogicalSourceResolver.ExpressionEvaluatorFactory<T> expressionEvaluatorFactory
	) {
		this.subjectGenerator = subjectGenerator;
		this.getIterator = getIterator;
		this.expressionEvaluatorFactory = expressionEvaluatorFactory;
	}
	

	Set<Resource> map(Map<String, Object> joinValues) {
		Set<Resource> results = new LinkedHashSet<>();
		getIterator.get().forEach(e ->
			map(e, joinValues)
				.ifPresent(results::add));
		return results;
	}
	
	private Optional<Resource> map(T entry, Map<String, Object> joinValues) {
		// example of joinValues: key: "$.country.name", value: "Belgium"
		EvaluateExpression evaluate =
			expressionEvaluatorFactory.apply(entry);
		boolean isValidJoin = joinValues.keySet().stream().allMatch(parentExpression -> {
			Optional<Object> parentValue = evaluate.apply(parentExpression);
			Object requiredValue = joinValues.get(parentExpression);
			return parentValue
					.map(p -> Objects.equals(p, requiredValue))
					.orElse(false);
		});
		 if (isValidJoin) {
			 return subjectGenerator.apply(evaluate);
		 }
		 return Optional.empty();
		
	}
}
