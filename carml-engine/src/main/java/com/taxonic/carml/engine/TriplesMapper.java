package com.taxonic.carml.engine;

import org.eclipse.rdf4j.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

class TriplesMapper<T> {

	private static final Logger LOG = LoggerFactory.getLogger(TriplesMapper.class);

	private final String name;
	private final Supplier<Stream<Item<T>>> getStream;
	private final SubjectMapper subjectMapper;
	private final Set<NestedMapper<T>> nestedMappers;

	TriplesMapper(
		String name,
		Supplier<Stream<Item<T>>> getStream,
		SubjectMapper subjectMapper,
		Set<NestedMapper<T>> nestedMappers
	) {
		this.name = name;
		this.getStream = getStream;
		this.subjectMapper = subjectMapper;
		this.nestedMappers = nestedMappers;
	}

	void map(Model model) {
		LOG.debug("Executing TriplesMap {} ...", name);
		getStream.get().forEach(e -> map(e, model));
	}

	private void map(Item<T> entry, Model model) {
		LOG.trace("Mapping triples for entry {}", entry.getItem());
		EvaluateExpression evaluate = entry.getEvaluate();
		subjectMapper.map(model, evaluate);
		nestedMappers.forEach(n -> n.map(model, evaluate));
	}
}
