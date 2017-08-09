package com.taxonic.rml.model.impl;

import java.util.Set;

import org.eclipse.rdf4j.model.IRI;

import com.taxonic.rml.model.Join;
import com.taxonic.rml.model.RefObjectMap;
import com.taxonic.rml.model.TriplesMap;

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
	
	//TO DO @RdfProperty(Rr.parentTriplesMap)
	//TO DO @RdfType(ParentTriplesMapImpl.class)
	@Override
	public TriplesMap getParentTriplesMap() {
		return parentTriplesMap;
	}

	//TO DO @RdfPropery(Rr.joinCondition)
	//TO DO @RdfType(JoinImpl.class)
	@Override
	public Set<Join> getJoinConditions() {
		return joinConditions;
	}
}
