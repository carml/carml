package com.taxonic.carml.engine;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PredicateMapper {

	private static final Logger LOG = LoggerFactory.getLogger(PredicateMapper.class);
	
	private TermGenerator<IRI> generator;
	private Set<TermGenerator<? extends Value>> objectGenerators;
	private Set<RefObjectMapper> refObjectMappers;
	
	PredicateMapper(
		TermGenerator<IRI> generator,
		Set<TermGenerator<? extends Value>> objectGenerators,
		Set<RefObjectMapper> refObjectMappers
	) {
		this.generator = generator;
		this.objectGenerators = objectGenerators;
		this.refObjectMappers = refObjectMappers;
	}

	void map(Model model, EvaluateExpression evaluate, Resource subject, Resource... contexts) {
		
		List<IRI> predicate = generator.apply(evaluate);
		predicate.stream().findFirst().ifPresent(p -> mapPredicate(p, model, evaluate, subject, contexts));
	}
	
	private void mapPredicate(IRI predicate, Model model, EvaluateExpression evaluate, Resource subject, Resource... contexts) {
		LOG.debug("Mapping objects for predicate {} for subject {}, in contexts {}", predicate, subject, contexts);
		Consumer<Value> addObjectTriple =
				o -> {
					LOG.debug("Adding triple {} {} {} {} to model", subject, predicate, o, contexts);
					model.add(subject, predicate, o, contexts);
				} ;
			
		objectGenerators.stream()
			.map(g -> g.apply(evaluate))
			.flatMap(List::stream)
			.forEach(addObjectTriple);
		
		refObjectMappers.stream()
			.flatMap(r -> r.map(evaluate).stream())
			.forEach(addObjectTriple);
	}
}
