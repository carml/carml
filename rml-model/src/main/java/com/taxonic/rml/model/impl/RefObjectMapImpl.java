package com.taxonic.rml.model.impl;

import java.util.LinkedHashSet;
import java.util.Set;

import com.taxonic.rml.model.Join;
import com.taxonic.rml.model.RefObjectMap;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.SubjectMapImpl.Builder;
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
	@RdfType(JoinImpl.class)
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
		return "RefObjectMapImpl [getParentTriplesMap()=" + getParentTriplesMap() 
			+ ", getJoinConditions()=" + getJoinConditions() + "] ";
	}
	
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((joinConditions == null) ? 0 : joinConditions.hashCode());
		result = prime * result + ((parentTriplesMap == null) ? 0 : parentTriplesMap.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		RefObjectMapImpl other = (RefObjectMapImpl) obj;
		if (joinConditions == null) {
			if (other.joinConditions != null) return false;
		}
		else if (!joinConditions.equals(other.joinConditions)) return false;
		if (parentTriplesMap == null) {
			if (other.parentTriplesMap != null) return false;
		}
		else if (!parentTriplesMap.equals(other.parentTriplesMap)) return false;
		return true;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder{
		
		private TriplesMap parentTriplesMap;
		private Set<Join> joinConditions = new LinkedHashSet<>();
		
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
		
		public RefObjectMapImpl build() {
			return new RefObjectMapImpl(
					parentTriplesMap,
					joinConditions
			);
		}
	}
}
