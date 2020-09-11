package com.taxonic.carml.engine;

import java.util.Set;
import java.util.stream.Stream;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

import com.taxonic.carml.vocab.Rdf.Rr;

class PredicateObjectMapper {
	
	private Set<TermGenerator<IRI>> graphGenerators;
	private Set<PredicateMapper> predicateMappers;

	PredicateObjectMapper(
		Set<TermGenerator<IRI>> graphGenerators,
		Set<PredicateMapper> predicateMappers
	) {
		this.graphGenerators = graphGenerators;
		this.predicateMappers = predicateMappers;
	}

	void map(Model model, EvaluateExpression evaluate, Resource subject, Set<IRI> subjectGraphs) {

		Resource[] contexts = Stream
				.concat(
					subjectGraphs.stream(),
					graphGenerators.stream()
							.map(g -> g.apply(evaluate))
							.filter(l -> !l.isEmpty())
							.map(l -> l.get(0))
				)
				.distinct()
				.filter(g -> !g.equals(Rr.defaultGraph))
				.toArray(Resource[]::new);
		
		predicateMappers.forEach(p -> p.map(model, evaluate, subject, contexts));
		
	}
}
