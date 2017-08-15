package com.taxonic.rml.engine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.model.Resource;

import com.taxonic.rml.model.Join;

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

	List<Resource> map(EvaluateExpression evaluate) {
		Map<String, Object> joinValues = createJoinValues(evaluate);
		return parentTriplesMapper.map(joinValues);
	}
	
	private Map<String, Object> createJoinValues(EvaluateExpression evaluate) {
		Map<String, Object> joinValues = new LinkedHashMap<>();
		joinConditions.stream().forEach(j -> {
			Object childValue = evaluate.apply(j.getChildReference());
			joinValues.put(j.getParentReference(), childValue);
		});
		return joinValues;
	}

}
