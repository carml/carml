package com.taxonic.carml.engine;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.eclipse.rdf4j.model.Resource;

import com.taxonic.carml.model.Join;

public class RefObjectMapper {
	
	private ParentTriplesMapper parentTriplesMapper;
	private Set<Join> joinConditions;
	
	RefObjectMapper(
		ParentTriplesMapper parentTriplesMapper,
		Set<Join> joinConditions
	) {
		this.parentTriplesMapper = parentTriplesMapper;
		this.joinConditions = joinConditions;
	}

	Set<Resource> map(EvaluateExpression evaluate) {
		Map<String, Object> joinValues = createJoinValues(evaluate);
		return parentTriplesMapper.map(joinValues);
	}
	
	private Map<String, Object> createJoinValues(EvaluateExpression evaluate) {
		Map<String, Object> joinValues = new LinkedHashMap<>();
		joinConditions.stream().forEach(j -> {
			Optional<Object> childValue = evaluate.apply(j.getChildReference());
			childValue.ifPresent(c -> joinValues.put(j.getParentReference(), c));
			//TODO: PM: what should happen when there is no childValue found?
			// log a warning? and ignore the mapping?
		});
		return joinValues;
	}

}
