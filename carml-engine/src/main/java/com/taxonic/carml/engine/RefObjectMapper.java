package com.taxonic.carml.engine;

import java.util.HashSet;
import java.util.Set;

import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver;
import com.taxonic.carml.logical_source_resolver.LogicalSourceResolver.CreateSimpleTypedRepresentation;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Resource;

import com.taxonic.carml.model.Join;

import static java.util.Collections.emptyList;

public class RefObjectMapper {
	
	private final ParentTriplesMapper<?> parentTriplesMapper;
	private final Set<Join> joinConditions;
	private final CreateSimpleTypedRepresentation createSimpleTypedRepresentation;

	RefObjectMapper(
		ParentTriplesMapper<?> parentTriplesMapper,
		Set<Join> joinConditions,
		CreateSimpleTypedRepresentation createSimpleTypedRepresentation
	) {
		this.parentTriplesMapper = parentTriplesMapper;
		this.joinConditions = joinConditions;
		this.createSimpleTypedRepresentation = createSimpleTypedRepresentation;
	}

	Set<Resource> map(EvaluateExpression evaluate) {
		Set<Pair<String, Object>> joinValues = createJoinValues(evaluate);
		return parentTriplesMapper.map(joinValues);
	}
	
	private Set<Pair<String, Object>> createJoinValues(EvaluateExpression evaluate) {
		Set<Pair<String, Object>> joinValues = new HashSet<>();
		joinConditions.forEach(j -> {
			Object childValue = evaluate.apply(j.getChildReference())
				.map(createSimpleTypedRepresentation).orElse(emptyList());
			joinValues.add(Pair.of(j.getParentReference(), childValue));
		});
		return joinValues;
	}
}
