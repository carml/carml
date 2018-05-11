package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Rr;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class CarmlRefObjectMap implements RefObjectMap {

	TriplesMap parentTriplesMap;
	Set<Join> joinConditions;

	public CarmlRefObjectMap() {}

	public CarmlRefObjectMap(
			TriplesMap parentTriplesMap,
			Set<Join> joinConditions
		) {
			this.parentTriplesMap = parentTriplesMap;
			this.joinConditions = joinConditions;
	}

	@RdfProperty(Rr.parentTriplesMap)
	@RdfType(CarmlTriplesMap.class)
	@Override
	public TriplesMap getParentTriplesMap() {
		return parentTriplesMap;
	}

	@RdfProperty(Rr.joinCondition)
	@RdfType(CarmlJoin.class)
	@Override
	public Set<Join> getJoinConditions() {
		return joinConditions;
	}

	public void setParentTriplesMap(TriplesMap parentTriplesMap) {
		this.parentTriplesMap = parentTriplesMap;
	}

	public void setJoinConditions(Set<Join> joinConditions) {
		this.joinConditions = joinConditions;
	}

	@Override
	public String toString() {
		return "CarmlRefObjectMap [getParentTriplesMap()=" + getParentTriplesMap()
			+ ", getJoinConditions()=" + getJoinConditions() + "] ";
	}



	@Override
	public int hashCode() {
		return Objects.hash(parentTriplesMap, joinConditions);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		CarmlRefObjectMap other = (CarmlRefObjectMap) obj;
		return Objects.equals(parentTriplesMap, other.parentTriplesMap) &&
				Objects.equals(joinConditions, other.joinConditions);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder{

		TriplesMap parentTriplesMap;
		Set<Join> joinConditions = new LinkedHashSet<>();

		Builder() {}

		public Builder parentTriplesMap(TriplesMap parentTriplesMap) {
			this.parentTriplesMap = parentTriplesMap;
			return this;
		}

		public Builder joinConditions(Set<Join> joinConditions) {
			this.joinConditions = joinConditions;
			return this;
		}

		public Builder condition(Join condition) {
			joinConditions.add(condition);
			return this;
		}

		public CarmlRefObjectMap build() {
			return new CarmlRefObjectMap(
					parentTriplesMap,
					joinConditions
			);
		}
	}
}
