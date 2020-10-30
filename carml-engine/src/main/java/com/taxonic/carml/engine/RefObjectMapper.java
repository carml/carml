package com.taxonic.carml.engine;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Resource;

import com.taxonic.carml.model.Join;

import static java.util.Collections.emptyList;

public class RefObjectMapper {
	
	private final ParentTriplesMapper<?> parentTriplesMapper;
	private final Set<Join> joinConditions;
	
	RefObjectMapper(
		ParentTriplesMapper<?> parentTriplesMapper,
		Set<Join> joinConditions
	) {
		this.parentTriplesMapper = parentTriplesMapper;
		this.joinConditions = joinConditions;
	}

	Set<Resource> map(EvaluateExpression evaluate) {
		Set<Pair<String, Object>> joinValues = createJoinValues(evaluate);
		return parentTriplesMapper.map(joinValues);
	}
	
	private Set<Pair<String, Object>> createJoinValues(EvaluateExpression evaluate) {
		Set<Pair<String, Object>> joinValues = new HashSet<>();
		joinConditions.forEach(j -> {
			Object childValue = evaluate.apply(j.getChildReference()).orElse(emptyList());
			joinValues.add(Pair.of(j.getParentReference(), childValue));
		});
		return joinValues;
	}
}
