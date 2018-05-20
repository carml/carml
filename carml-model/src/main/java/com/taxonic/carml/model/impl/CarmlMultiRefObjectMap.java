package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.MultiRefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Carml;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

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
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
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
