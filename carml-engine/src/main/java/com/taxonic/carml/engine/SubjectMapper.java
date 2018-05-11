package com.taxonic.carml.engine;

import com.taxonic.carml.rdf_mapper.util.ImmutableCollectors;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SubjectMapper {

	private static final Logger LOG = LoggerFactory.getLogger(SubjectMapper.class);

	private TermGenerator<Resource> generator;
	private Set<TermGenerator<IRI>> graphGenerators;
	private Set<IRI> classes;
	private Set<PredicateObjectMapper> predicateObjectMappers;

	SubjectMapper(
		TermGenerator<Resource> generator,
		Set<TermGenerator<IRI>> graphGenerators,
		Set<IRI> classes,
		Set<PredicateObjectMapper> predicateObjectMappers
	) {
		this.generator = generator;
		this.graphGenerators = graphGenerators;
		this.classes = classes;
		this.predicateObjectMappers = predicateObjectMappers;
	}

	Optional<Resource> map(Model model, EvaluateExpression evaluate) {
		LOG.debug("Determining subjects ...");
		List<Resource> subject = generator.apply(evaluate);
		LOG.debug("Determined subjects {}", subject);
		return subject.stream().findFirst().map(s -> mapSubject(s, model, evaluate));
	}

	private Resource mapSubject(Resource subject, Model model, EvaluateExpression evaluate) {
		// use graphs when generating statements later
		LOG.debug("Generating triples for subject: {}", subject);
		Set<IRI> graphs = graphGenerators.stream()
			.map(g -> g.apply(evaluate))
			.filter(l -> !l.isEmpty())
			.map(l -> l.get(0))
			.collect(ImmutableCollectors.toImmutableSet());

		Resource[] contexts = new Resource[graphs.size()];
		graphs.toArray(contexts);

		// generate rdf:type triples from classes
		classes.forEach(c -> model.add(subject, RDF.TYPE, c, contexts));

		predicateObjectMappers.forEach(p -> p.map(model, evaluate, subject, graphs));

		return subject;
	}
}
