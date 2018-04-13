package com.taxonic.carml.model.impl;

import com.taxonic.carml.model.BaseObjectMap;
import com.taxonic.carml.model.GraphMap;
import com.taxonic.carml.model.PredicateMap;
import com.taxonic.carml.model.PredicateObjectMap;
import com.taxonic.carml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.carml.rdf_mapper.annotations.RdfType;
import com.taxonic.carml.rdf_mapper.annotations.RdfTypeDecider;
import com.taxonic.carml.vocab.Rr;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class CarmlPredicateObjectMap implements PredicateObjectMap {

	private Set<PredicateMap> predicateMaps;
	private Set<BaseObjectMap> objectMaps;
	private Set<GraphMap> graphMaps;

	public CarmlPredicateObjectMap() {}

	public CarmlPredicateObjectMap(
		Set<PredicateMap> predicateMaps,
		Set<BaseObjectMap> objectMaps,
		Set<GraphMap> graphMaps
	) {
		this.predicateMaps = predicateMaps;
		this.objectMaps = objectMaps;
		this.graphMaps = graphMaps;
	}

	@RdfProperty(Rr.predicateMap)
	@RdfType(CarmlPredicateMap.class)
	@Override
	public Set<PredicateMap> getPredicateMaps() {
		return predicateMaps;
	}

	// TODO: make @RdfProperty repeatable so we can also use Carml.multiObjectMap here
	@RdfProperty(Rr.objectMap)
	@RdfTypeDecider(ObjectMapTypeDecider.class)
	@Override
	public Set<BaseObjectMap> getObjectMaps() {
		return objectMaps;
	}

	@RdfProperty(Rr.graphMap)
	@RdfType(CarmlGraphMap.class)
	@Override
	public Set<GraphMap> getGraphMaps() {
		return graphMaps;
	}

	public void setPredicateMaps(Set<PredicateMap> predicateMaps) {
		this.predicateMaps = predicateMaps;
	}

	public void setObjectMaps(Set<BaseObjectMap> objectMaps) {
		this.objectMaps = objectMaps;
	}

	public void setGraphMaps(Set<GraphMap> graphMaps) {
		this.graphMaps = graphMaps;
	}

	@Override
	public String toString() {
		return "CarmlPredicateObjectMap [getPredicateMaps()=" + getPredicateMaps() + ", getObjectMaps()="
			+ getObjectMaps() + ", getGraphMaps()=" + getGraphMaps() + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(predicateMaps, objectMaps, graphMaps);
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
		CarmlPredicateObjectMap other = (CarmlPredicateObjectMap) obj;
		return Objects.equals(predicateMaps, other.predicateMaps) &&
				Objects.equals(objectMaps, other.objectMaps) &&
				Objects.equals(graphMaps, other.graphMaps);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {

		private Set<PredicateMap> predicateMaps = new LinkedHashSet<>();
		private Set<BaseObjectMap> objectMaps = new LinkedHashSet<>();
		private Set<GraphMap> graphMaps = new LinkedHashSet<>();

		public Builder predicateMap(PredicateMap predicateMap) {
			predicateMaps.add(predicateMap);
			return this;
		}

		public Builder objectMap(BaseObjectMap objectMap) {
			objectMaps.add(objectMap);
			return this;
		}

		public Builder graphMap(CarmlGraphMap carmlGraphMap) {
			graphMaps.add(carmlGraphMap);
			return this;
		}

		public Builder graphMaps(Set<GraphMap> graphMaps) {
			this.graphMaps= graphMaps;
			return this;
		}

		public CarmlPredicateObjectMap build() {
			return new CarmlPredicateObjectMap(
				predicateMaps,
				objectMaps,
				graphMaps
			);
		}
	}
}
