package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.MultiRefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Carml;
import java.util.Set;

public class CarmlMultiRefObjectMap extends CarmlRefObjectMap implements MultiRefObjectMap {

	public CarmlMultiRefObjectMap() {}
	
	public CarmlMultiRefObjectMap(
		TriplesMap parentTriplesMap,
		Set<Join> joinConditions
	) {
		super(parentTriplesMap, joinConditions);
	}
	
	@RdfProperty(Carml.multiJoinCondition)
	@RdfType(CarmlJoin.class)
	@Override
	public Set<Join> getJoinConditions() {
		return joinConditions;
	}
	
	@Override
	public String toString() {
		return "CarmlMultiRefObjectMap [parentTriplesMap=" + parentTriplesMap + ", joinConditions="
				+ joinConditions + "]";
	}

	public static class Builder extends CarmlRefObjectMap.Builder {
		
		@Override
		public CarmlRefObjectMap build() {
			return new CarmlMultiRefObjectMap(
					parentTriplesMap,
					joinConditions
			);
		}
	}
}
