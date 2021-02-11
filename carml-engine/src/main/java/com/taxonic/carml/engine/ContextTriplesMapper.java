package com.taxonic.carml.engine;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.GetStreamFromContext;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

class ContextTriplesMapper<T> {

	private static final Logger LOG = LoggerFactory.getLogger(ContextTriplesMapper.class);

	private final String name;
	private final GetStreamFromContext<T> getStreamFromContext;
	private final SubjectMapper subjectMapper;
	private final Set<NestedMapper<T>> nestedMappers;

	ContextTriplesMapper(
		String name,
		GetStreamFromContext<T> getStreamFromContext,
		SubjectMapper subjectMapper,
		Set<NestedMapper<T>> nestedMappers
	) {
		this.name = name;
		this.getStreamFromContext = getStreamFromContext;
		this.subjectMapper = subjectMapper;
		this.nestedMappers = nestedMappers;
	}

	Set<Resource> map(Model model, EvaluateExpression evaluateInContext) {
		LOG.debug("Executing context TriplesMap {} ...", name);
		Stream<Item<T>> entries = getStreamFromContext.apply(evaluateInContext);
		// TODO if specified, the 'per entry' context must still be constructed here,
		//      by doing evaluations with 'evaluateInContext'. this allows for selecting and renaming context vars,
		//      or doing a deeper selection.
		//      so we have...
		//      1. an 'iteration context'
		//      2. an 'entry context'
		//      in the default case (no config), the entry context is simply set to the iteration context.
		Set<Resource> results = new LinkedHashSet<>();
		entries.forEach(e -> map(e, model, evaluateInContext).ifPresent(results::add));
		return Collections.unmodifiableSet(results);
	}

	private Optional<Resource> map(Item<T> entry, Model model, EvaluateExpression evaluateInContext) {
		LOG.trace("Mapping triples for entry {}", entry.getItem());
		EvaluateExpression evaluateInEntry = entry.getEvaluate();
		EvaluateExpression evaluate = e -> {
			Optional<Object> result = evaluateInEntry.apply(e);
			if (result.isPresent()) {
				return result;
			}
			return evaluateInContext.apply(e);
		};
		Optional<Resource> subject = subjectMapper.map(model, evaluate);
		nestedMappers.forEach(n -> n.map(model, evaluate));
		return subject;
	}
}
