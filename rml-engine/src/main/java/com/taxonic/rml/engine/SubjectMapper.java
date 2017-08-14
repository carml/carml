package com.taxonic.rml.engine;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

class SubjectMapper {
	
	private TermGenerator<Resource> generator;
	private List<TermGenerator<IRI>> graphGenerators;
	private Set<IRI> classes;
	private List<PredicateObjectMapper> predicateObjectMappers;
	
	SubjectMapper(
		TermGenerator<Resource> generator,
		List<TermGenerator<IRI>> graphGenerators,
		Set<IRI> classes,
		List<PredicateObjectMapper> predicateObjectMappers
	) {
		this.generator = generator;
		this.graphGenerators = graphGenerators;
		this.classes = classes;
		this.predicateObjectMappers = predicateObjectMappers;
	}

	void map(Model model, EvaluateExpression evaluate) {
		
		Resource subject = generator.apply(evaluate);
		if (subject == null) return;
		
		// use graphs when generating statements later
		List<IRI> graphs = graphGenerators.stream()
			.map(g -> g.apply(evaluate))
			.collect(Collectors.toList());
		
		// generate rdf:type triples from classes
		classes.forEach(c -> model.add(subject, RDF.TYPE, c));
		
		predicateObjectMappers.forEach(p -> p.map(model, evaluate, subject, graphs));
	}
}
