package com.taxonic.carml.engine;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Resource;

import com.taxonic.carml.model.Join;

public class RefObjectMapper {
	
	private ParentTriplesMapper<?> parentTriplesMapper;
	private Set<Join> joinConditions;
	
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
		joinConditions.stream().forEach(j -> {
			Optional<Object> childValue = evaluate.apply(j.getChildReference());
			childValue.ifPresent(c -> joinValues.add(Pair.of(j.getParentReference(), c)));
		});
		return joinValues;
	}
}
