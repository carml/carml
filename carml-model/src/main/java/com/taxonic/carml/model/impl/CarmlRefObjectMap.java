package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.model.Join;
import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.Resource;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.vocab.Carml;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rr;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class CarmlRefObjectMap extends CarmlResource implements RefObjectMap {

	TriplesMap parentTriplesMap;
	Set<Join> joinConditions;

	public CarmlRefObjectMap() {
		// Empty constructor for object mapper
	}

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
	@RdfProperty(value = Carml.multiJoinCondition, deprecated = true)
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
		return new ReflectionToStringBuilder(this, new MultilineRecursiveToStringStyle()).toString();
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

	@Override
	public Set<Resource> getReferencedResources() {
		ImmutableSet.Builder<Resource> builder = ImmutableSet.<Resource>builder();
		if (parentTriplesMap != null) {
			builder.add(parentTriplesMap);
		}
		return builder.addAll(joinConditions)
				.build();
	}

	@Override
	public void addTriples(ModelBuilder modelBuilder) {
		modelBuilder.subject(getAsResource())
				.add(RDF.TYPE, Rdf.Rr.RefObjectMap);
		if (parentTriplesMap != null) {
			modelBuilder.add(Rr.parentTriplesMap, parentTriplesMap.getAsResource());
		}
		joinConditions.forEach(jc -> modelBuilder.add(Rr.joinCondition, jc.getAsResource()));
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
