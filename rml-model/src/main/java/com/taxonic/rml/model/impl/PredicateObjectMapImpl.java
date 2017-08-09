package com.taxonic.rml.model.impl;

import java.util.LinkedHashSet;
import java.util.Set;

import com.taxonic.rml.model.BaseObjectMap;
import com.taxonic.rml.model.GraphMap;
import com.taxonic.rml.model.ObjectMap;
import com.taxonic.rml.model.PredicateMap;
import com.taxonic.rml.model.PredicateObjectMap;
import com.taxonic.rml.model.impl.SubjectMapImpl.Builder;
import com.taxonic.rml.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rml.rdf_mapper.annotations.RdfType;
import com.taxonic.rml.rdf_mapper.annotations.RdfTypeDecider;
import com.taxonic.rml.vocab.Rr;

public class PredicateObjectMapImpl implements PredicateObjectMap {

	private Set<PredicateMap> predicateMaps;
	private Set<BaseObjectMap> objectMaps;
	private Set<GraphMap> graphMaps;

	public PredicateObjectMapImpl() {}
	
	public PredicateObjectMapImpl(
		Set<PredicateMap> predicateMaps,
		Set<BaseObjectMap> objectMaps,
		Set<GraphMap> graphMaps
	) {
		this.predicateMaps = predicateMaps;
		this.objectMaps = objectMaps;
		this.graphMaps = graphMaps;
	}

	@RdfProperty(Rr.predicateMap)
	@RdfType(PredicateMapImpl.class)
	@Override
	public Set<PredicateMap> getPredicateMaps() {
		return predicateMaps;
	}

	@RdfProperty(Rr.objectMap)
	@RdfTypeDecider(ObjectMapTypeDecider.class)
	@Override
	public Set<BaseObjectMap> getObjectMaps() {
		return objectMaps;
	}
	
	@RdfProperty(Rr.graphMap)
	@RdfType(GraphMapImpl.class)
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
		return "PredicateObjectMapImpl [getPredicateMaps()=" + getPredicateMaps() + ", getObjectMaps()="
			+ getObjectMaps() + ", getGraphMaps()=" + getGraphMaps() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((objectMaps == null) ? 0 : objectMaps.hashCode());
		result = prime * result + ((predicateMaps == null) ? 0 : predicateMaps.hashCode());
		result = prime * result + ((graphMaps == null) ? 0 : graphMaps.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		PredicateObjectMapImpl other = (PredicateObjectMapImpl) obj;
		if (objectMaps == null) {
			if (other.objectMaps != null) return false;
		}
		else if (!objectMaps.equals(other.objectMaps)) return false;
		if (predicateMaps == null) {
			if (other.predicateMaps != null) return false;
		}
		else if (!predicateMaps.equals(other.predicateMaps)) return false;
		if (graphMaps == null) {
			if (other.graphMaps != null) return false;
		}
		else if (!graphMaps.equals(other.graphMaps)) return false;
		return true;
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
		
		public Builder graphMap(GraphMapImpl graphMapImpl) {
			graphMaps.add(graphMapImpl);
			return this;
		}
		
		public Builder graphMaps(Set<GraphMap> graphMaps) {
			this.graphMaps= graphMaps;
			return this;
		}
		
		public PredicateObjectMapImpl build() {
			return new PredicateObjectMapImpl(
				predicateMaps,
				objectMaps,
				graphMaps
			);
		}
	}
}
