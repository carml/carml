package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;

import java.util.Set;
import java.util.function.Supplier;
import org.eclipse.rdf4j.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TriplesMapper<T> {

	private static final Logger LOG = LoggerFactory.getLogger(TriplesMapper.class);

	private String name;
	private Supplier<Iterable<T>> getIterator;
	private LogicalSourceResolver.ExpressionEvaluatorFactory<T> expressionEvaluatorFactory;
	private SubjectMapper subjectMapper;
	private Set<NestedMapper<T>> nestedMappers;

	TriplesMapper(
		TriplesMapperComponents<T> trMapperComponents,
		SubjectMapper subjectMapper,
		Set<NestedMapper<T>> nestedMappers
	) {
		this(
			trMapperComponents.getName(),
			trMapperComponents.getIterator(),
			trMapperComponents.getExpressionEvaluatorFactory(),
			subjectMapper,
			nestedMappers
		);
	}

	TriplesMapper(
		String name,
		Supplier<Iterable<T>> getIterator,
		LogicalSourceResolver.ExpressionEvaluatorFactory<T> expressionEvaluatorFactory,
		SubjectMapper subjectMapper,
		Set<NestedMapper<T>> nestedMappers
	) {
		this.name = name;
		this.getIterator = getIterator;
		this.expressionEvaluatorFactory = expressionEvaluatorFactory;
		this.subjectMapper = subjectMapper;
		this.nestedMappers = nestedMappers;
	}

	void map(Model model) {
		LOG.debug("Executing TriplesMap {} ...", name);
		Iterable<T> iter = getIterator.get();
		iter.forEach(e -> map(e, model));
	}

	private void map(T entry, Model model) {
		LOG.trace("Mapping triples for entry {}", entry);
		EvaluateExpression evaluate =
			expressionEvaluatorFactory.apply(entry);
		subjectMapper.map(model, evaluate); // TODO probably pass resulting resource to any nested mappers
		nestedMappers.forEach(n -> n.map(model, evaluate));
	}
}
