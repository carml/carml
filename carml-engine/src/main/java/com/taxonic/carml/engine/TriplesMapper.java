package com.taxonic.carml.engine;

import com.taxonic.carml.resolvers.LogicalSourceResolver;

import java.util.function.Supplier;

import org.eclipse.rdf4j.model.Model;

class TriplesMapper<SourceType> {
	
	private Supplier<Iterable<SourceType>> getIterator;
	private LogicalSourceResolver.ExpressionEvaluatorFactory<SourceType> expressionEvaluatorFactory;
	private SubjectMapper subjectMapper;
	
	TriplesMapper(
		Supplier<Iterable<SourceType>> getIterator,
		LogicalSourceResolver.ExpressionEvaluatorFactory<SourceType> expressionEvaluatorFactory,
		SubjectMapper subjectMapper
	) {
		this.getIterator = getIterator;
		this.expressionEvaluatorFactory = expressionEvaluatorFactory;
		this.subjectMapper = subjectMapper;
	}
	
	void map(Model model) {
		getIterator.get().forEach(e -> map(e, model));
	}
	
	private void map(SourceType entry, Model model) {
		EvaluateExpression evaluate =
			expressionEvaluatorFactory.apply(entry);
		subjectMapper.map(model, evaluate);
	}
}
