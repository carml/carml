package com.taxonic.rml.engine;

import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

class PredicateMapper {
	
	private TermGenerator<IRI> generator;
	private List<TermGenerator<Value>> objectGenerators;
	
	PredicateMapper(
		TermGenerator<IRI> generator,
		List<TermGenerator<Value>> objectGenerators
	) {
		this.generator = generator;
		this.objectGenerators = objectGenerators;
	}

	void map(Model model, EvaluateExpression evaluate, Resource subject, Resource... contexts) {
		
		IRI predicate = generator.apply(evaluate);
		if (predicate == null) return;
		
		objectGenerators.stream()
			.map(g -> g.apply(evaluate))
			.filter(o -> o != null)
			.forEach(o -> model.add(subject, predicate, o, contexts));
		
	}
}
