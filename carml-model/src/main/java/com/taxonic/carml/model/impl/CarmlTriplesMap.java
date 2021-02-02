package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.NestedMapping;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.SubjectMap;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.CarmlExp;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rml;
import com.taxonic.carml.vocab.Rr;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlTriplesMap extends CarmlResource implements TriplesMap{

	private LogicalSource logicalSource;
	private SubjectMap subjectMap;
	private Set<PredicateObjectMap> predicateObjectMaps;
	private Set<NestedMapping> nestedMappings;

	public CarmlTriplesMap() {
		// Empty constructor for object mapper
	}

	public CarmlTriplesMap(
		LogicalSource logicalSource,
		SubjectMap subjectMap,
		Set<PredicateObjectMap> predicateObjectMaps,
		Set<NestedMapping> nestedMappings
	) {
		this.logicalSource = logicalSource;
		this.subjectMap = subjectMap;
		this.predicateObjectMaps = predicateObjectMaps;
		this.nestedMappings = nestedMappings;
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

	@RdfProperty(CarmlExp.nestedMapping)
	@RdfType(CarmlNestedMapping.class)
	@Override
	public Set<NestedMapping> getNestedMappings() {
		return nestedMappings;
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

	public void setNestedMappings(Set<NestedMapping> nestedMappings) {
		this.nestedMappings = nestedMappings;
	}

	@Override
	public String toString() {
		ToStringStyle style = new MultilineRecursiveToStringStyle();
		StringBuffer buffer = new StringBuffer();
		buffer.append(String.format("%s %s:%n ", getClass().getSimpleName(), getResourceName()));
		return new ReflectionToStringBuilder(this, style, buffer).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(logicalSource, subjectMap, predicateObjectMaps, nestedMappings);
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
				Objects.equals(predicateObjectMaps, other.predicateObjectMaps) &&
				Objects.equals(nestedMappings, other.nestedMappings);
	}

	@Override
	public Set<Resource> getReferencedResources() {
		ImmutableSet.Builder<Resource> builder = ImmutableSet.<Resource>builder();
		if (logicalSource != null) {
			builder.add(logicalSource);
		}
		if (subjectMap != null) {
			builder.add(subjectMap);
		}
		return builder
			.addAll(predicateObjectMaps)
			.addAll(nestedMappings)
			.build();
	}

	@Override
	public void addTriples(ModelBuilder modelBuilder) {
		modelBuilder.subject(getAsResource())
				.add(RDF.TYPE, Rdf.Rr.TriplesMap);
		if (logicalSource != null) {
			modelBuilder.add(Rml.logicalSource, logicalSource.getAsResource());
		}
		if (subjectMap != null) {
			modelBuilder.add(Rr.subjectMap, subjectMap.getAsResource());
		}
		predicateObjectMaps.forEach(pom -> modelBuilder.add(Rr.predicateObjectMap, pom.getAsResource()));
		nestedMappings.forEach(n -> modelBuilder.add(CarmlExp.nestedMapping, n.getAsResource()));
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {

		private LogicalSource logicalSource;
		private SubjectMap subjectMap;
		private Set<PredicateObjectMap> predicateObjectMaps = new LinkedHashSet<>();
		private Set<NestedMapping> nestedMappings = new LinkedHashSet<>();

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

		public Builder nestedMapping(NestedMapping nestedMapping) {
			nestedMappings.add(nestedMapping);
			return this;
		}

		public CarmlTriplesMap build() {
			return new CarmlTriplesMap(
				logicalSource,
				subjectMap,
				predicateObjectMaps,
				nestedMappings
			);
		}
	}

}
