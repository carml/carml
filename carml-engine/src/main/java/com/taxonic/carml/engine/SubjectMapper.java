package com.taxonic.carml.engine;

import java.util.List;
import java.util.Optional;
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

	Optional<Resource> map(Model model, EvaluateExpression evaluate) {
		Optional<Resource> subject = generator.apply(evaluate);
		return subject.map(s -> mapSubject(s, model, evaluate));
	}
	
	private Resource mapSubject(Resource subject, Model model, EvaluateExpression evaluate) {
		// use graphs when generating statements later
		List<IRI> graphs = graphGenerators.stream()
			.map(g -> g.apply(evaluate))
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.toList());
		
		Resource[] contexts = new Resource[graphs.size()];
		graphs.toArray(contexts);
		
		// generate rdf:type triples from classes
		classes.forEach(c -> model.add(subject, RDF.TYPE, c, contexts));

		predicateObjectMappers.forEach(p -> p.map(model, evaluate, subject, graphs));
		
		return subject;
	}
}
