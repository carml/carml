package com.taxonic.rml.model.impl;

import java.util.LinkedHashSet;
import java.util.Set;

import com.taxonic.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rdf_mapper.annotations.RdfType;
import com.taxonic.rml.model.LogicalSource;
import com.taxonic.rml.model.PredicateObjectMap;
import com.taxonic.rml.model.SubjectMap;
import com.taxonic.rml.model.TriplesMap;
import com.taxonic.rml.model.impl.TriplesMapImpl.Builder;
import com.taxonic.rml.vocab.Rml;
import com.taxonic.rml.vocab.Rr;

public class TriplesMapImpl implements TriplesMap {

	private LogicalSource logicalSource;
	private SubjectMap subjectMap;
	private Set<PredicateObjectMap> predicateObjectMaps;
	
	public TriplesMapImpl() {}
	
	public TriplesMapImpl(
		LogicalSource logicalSource,
		SubjectMap subjectMap,
		Set<PredicateObjectMap> predicateObjectMaps
	) {
		this.logicalSource = logicalSource;
		this.subjectMap = subjectMap;
		this.predicateObjectMaps = predicateObjectMaps;
	}

	@RdfProperty(Rml.logicalSource)
	@RdfType(LogicalSourceImpl.class)
	@Override
	public LogicalSource getLogicalSource() {
		return logicalSource;
	}

	@RdfProperty(Rr.subjectMap)
	@RdfType(SubjectMapImpl.class)
	@Override
	public SubjectMap getSubjectMap() {
		return subjectMap;
	}

	@RdfProperty(Rr.predicateObjectMap)
	@RdfType(PredicateObjectMapImpl.class)
	@Override
	public Set<PredicateObjectMap> getPredicateObjectMaps() {
		return predicateObjectMaps;
	}

	public void setLogicalSource(LogicalSource logicalSource) {
		this.logicalSource = logicalSource;
	}

	public void setSubjectMap(SubjectMap subjectMap) {
		this.subjectMap = subjectMap;
	}

	public void setPredicateObjectMaps(Set<PredicateObjectMap> predicateObjectMaps) {
		this.predicateObjectMaps = predicateObjectMaps;
	}

	@Override
	public String toString() {
		return "TriplesMapImpl [getLogicalSource()=" + getLogicalSource() + ", getSubjectMap()=" + getSubjectMap()
			+ ", getPredicateObjectMaps()=" + getPredicateObjectMaps() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((logicalSource == null) ? 0 : logicalSource.hashCode());
		result = prime * result + ((predicateObjectMaps == null) ? 0 : predicateObjectMaps.hashCode());
		result = prime * result + ((subjectMap == null) ? 0 : subjectMap.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TriplesMapImpl other = (TriplesMapImpl) obj;
		if (logicalSource == null) {
			if (other.logicalSource != null) return false;
		}
		else if (!logicalSource.equals(other.logicalSource)) return false;
		if (predicateObjectMaps == null) {
			if (other.predicateObjectMaps != null) return false;
		}
		else if (!predicateObjectMaps.equals(other.predicateObjectMaps)) return false;
		if (subjectMap == null) {
			if (other.subjectMap != null) return false;
		}
		else if (!subjectMap.equals(other.subjectMap)) return false;
		return true;
	}

	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder {
		
		private LogicalSource logicalSource;
		private SubjectMap subjectMap;
		private Set<PredicateObjectMap> predicateObjectMaps = new LinkedHashSet<>();
		
		public Builder logicalSource(LogicalSource logicalSource) {
			this.logicalSource = logicalSource;
			return this;
		}
		
		public Builder subjectMap(SubjectMap subjectMap) {
			this.subjectMap = subjectMap;
			return this;
		}
		
		public Builder predicateObjectMap(PredicateObjectMap predicateObjectMap) {
			predicateObjectMaps.add(predicateObjectMap);
			return this;
		}

		public TriplesMapImpl build() {
			return new TriplesMapImpl(
				logicalSource,
				subjectMap,
				predicateObjectMaps
			);
		}
	}
	
}
