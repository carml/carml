package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Rml;
import com.taxonic.carml.vocab.Rr;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class CarmlTriplesMap extends CarmlResource implements TriplesMap{

	private LogicalSource logicalSource;
	private SubjectMap subjectMap;
	private Set<PredicateObjectMap> predicateObjectMaps;

	public CarmlTriplesMap() {}

	public CarmlTriplesMap(
		LogicalSource logicalSource,
		SubjectMap subjectMap,
		Set<PredicateObjectMap> predicateObjectMaps
	) {
		this.logicalSource = logicalSource;
		this.subjectMap = subjectMap;
		this.predicateObjectMaps = predicateObjectMaps;
	}

	@RdfProperty(Rml.logicalSource)
	@RdfType(CarmlLogicalSource.class)
	@Override
	public LogicalSource getLogicalSource() {
		return logicalSource;
	}

	@RdfProperty(Rr.subjectMap)
	@RdfType(CarmlSubjectMap.class)
	@Override
	public SubjectMap getSubjectMap() {
		return subjectMap;
	}

	@RdfProperty(Rr.predicateObjectMap)
	@RdfType(CarmlPredicateObjectMap.class)
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
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(logicalSource, subjectMap, predicateObjectMaps);
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
		CarmlTriplesMap other = (CarmlTriplesMap) obj;
		return Objects.equals(logicalSource, other.logicalSource) &&
				Objects.equals(subjectMap, other.subjectMap) &&
				Objects.equals(predicateObjectMaps, other.predicateObjectMaps);
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

		public CarmlTriplesMap build() {
			return new CarmlTriplesMap(
				logicalSource,
				subjectMap,
				predicateObjectMaps
			);
		}
	}

}
