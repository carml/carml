package com.taxonic.rml.model.impl;

import java.util.Set;

import com.taxonic.rml.model.Join;
import com.taxonic.rml.model.RefObjectMap;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rml.rdf_mapper.annotations.RdfType;
import com.taxonic.rml.vocab.Rr;

public class RefObjectMapImpl implements RefObjectMap {
	
	private TriplesMap parentTriplesMap;
	private Set<Join> joinConditions;
	
	public RefObjectMapImpl() {}
	
	public RefObjectMapImpl(
			TriplesMap parentTriplesMap,
			Set<Join> joinConditions
		) {
			this.parentTriplesMap = parentTriplesMap;
			this.joinConditions = joinConditions;
	}
	
	@RdfProperty(Rr.parentTriplesMap)
	@RdfType(TriplesMapImpl.class)
	@Override
	public TriplesMap getParentTriplesMap() {
		return parentTriplesMap;
	}

	@RdfProperty(Rr.joinCondition)
//	TODO @RdfType(JoinImpl.class)
	@Override
	public Set<Join> getJoinConditions() {
		return joinConditions;
	}
}
