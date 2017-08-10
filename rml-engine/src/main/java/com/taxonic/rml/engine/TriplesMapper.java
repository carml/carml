package com.taxonic.rml.engine;

import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.eclipse.rdf4j.model.Model;

class TriplesMapper {
	
	private Supplier<Object> getSource;
	private UnaryOperator<Object> applyIterator;
	private Function<Object, EvaluateExpression> expressionEvaluatorFactory;
	private SubjectMapper subjectMapper;
	
	TriplesMapper(
		Supplier<Object> getSource,
		UnaryOperator<Object> applyIterator,
		Function<Object, EvaluateExpression> expressionEvaluatorFactory,
		SubjectMapper subjectMapper
	) {
		this.getSource = getSource;
		this.applyIterator = applyIterator;
		this.expressionEvaluatorFactory = expressionEvaluatorFactory;
		this.subjectMapper = subjectMapper;
	}

	private Iterable<?> createIterable(Object value) {
		boolean isIterable = Iterable.class.isAssignableFrom(value.getClass());
		return isIterable
			? (Iterable<?>) value
			: Collections.singleton(value);
	}
	
	void map(Model model) {
		Object source = getSource.get();
		Object value = applyIterator.apply(source);
		Iterable<?> iterable = createIterable(value);
		iterable.forEach(e -> map(e, model));
	}
	
	private void map(Object entry, Model model) {
		EvaluateExpression evaluate =
			expressionEvaluatorFactory.apply(entry);
		subjectMapper.map(model, evaluate);
	}
}
