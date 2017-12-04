package com.taxonic.carml.engine;

import com.taxonic.carml.engine.TriplesMapperComponents;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;

import java.util.function.Supplier;

import org.eclipse.rdf4j.model.Model;

class TriplesMapper<T> {
	
	private Supplier<Iterable<T>> getIterator;
	private LogicalSourceResolver.ExpressionEvaluatorFactory<T> expressionEvaluatorFactory;
	private SubjectMapper subjectMapper;
	
	TriplesMapper(
		TriplesMapperComponents<T> trMapperComponents,
		SubjectMapper subjectMapper
	) {
		this(
			trMapperComponents.getIterator(), 
			trMapperComponents.getExpressionEvaluatorFactory(),
			subjectMapper
		);
	}
	
	TriplesMapper(
		Supplier<Iterable<T>> getIterator,
		LogicalSourceResolver.ExpressionEvaluatorFactory<T> expressionEvaluatorFactory,
		SubjectMapper subjectMapper
	) {
		this.getIterator = getIterator;
		this.expressionEvaluatorFactory = expressionEvaluatorFactory;
		this.subjectMapper = subjectMapper;
	}
	
	void map(Model model) {
		getIterator.get().forEach(e -> map(e, model));
	}
	
	private void map(T entry, Model model) {
		EvaluateExpression evaluate =
			expressionEvaluatorFactory.apply(entry);
		subjectMapper.map(model, evaluate);
	}
}
