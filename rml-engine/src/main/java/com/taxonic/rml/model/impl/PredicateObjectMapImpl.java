package com.taxonic.rml.model.impl;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

import com.taxonic.rdf_mapper.annotations.RdfProperty;
import com.taxonic.rdf_mapper.annotations.RdfType;
import com.taxonic.rml.model.ObjectMap;
import com.taxonic.rml.model.PredicateMap;
import com.taxonic.rml.model.PredicateObjectMap;
import com.taxonic.rml.vocab.Rr;

public class PredicateObjectMapImpl implements PredicateObjectMap {

	private Set<PredicateMap> predicateMaps;
	private Set<ObjectMap> objectMaps;

	public PredicateObjectMapImpl() {}
	
	public PredicateObjectMapImpl(
		Set<PredicateMap> predicateMaps,
		Set<ObjectMap> objectMaps
	) {
		this.predicateMaps = predicateMaps;
		this.objectMaps = objectMaps;
	}

	@RdfProperty(Rr.predicateMap)
	@RdfType(PredicateMapImpl.class)
	@Override
	public Set<PredicateMap> getPredicateMaps() {
		return predicateMaps;
	}

	// rr:predicate X is the shorthand for rr:predicateMap [ rr:constant X ]
	@RdfProperty(Rr.predicate)
	public void setPredicate(IRI predicate) {
		setPredicateMaps(
			Collections.singleton(
				PredicateMapImpl.newBuilder()
					.constant(predicate)
					.build()
			)
		);
	}
	
	@RdfProperty(Rr.objectMap)
	@RdfType(ObjectMapImpl.class)
	@Override
	public Set<ObjectMap> getObjectMaps() {
		return objectMaps;
	}
	
	// rr:object X is the shorthand for rr:objectMap [ rr:constant X ]
	@RdfProperty(Rr.object)
	public void setObject(Value object) {
		setObjectMaps(
			Collections.singleton(
				ObjectMapImpl.newBuilder()
					.constant(object)
					.build()
			)
		);
	}

	public void setPredicateMaps(Set<PredicateMap> predicateMaps) {
		this.predicateMaps = predicateMaps;
	}

	public void setObjectMaps(Set<ObjectMap> objectMaps) {
		this.objectMaps = objectMaps;
	}

	@Override
	public String toString() {
		return "PredicateObjectMapImpl [getPredicateMaps()=" + getPredicateMaps() + ", getObjectMaps()="
			+ getObjectMaps() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((objectMaps == null) ? 0 : objectMaps.hashCode());
		result = prime * result + ((predicateMaps == null) ? 0 : predicateMaps.hashCode());
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
		return true;
	}

	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder {

		private Set<PredicateMap> predicateMaps = new LinkedHashSet<>();
		private Set<ObjectMap> objectMaps = new LinkedHashSet<>();
		
		public Builder predicateMap(PredicateMap predicateMap) {
			predicateMaps.add(predicateMap);
			return this;
		}
		
		public Builder objectMap(ObjectMap objectMap) {
			objectMaps.add(objectMap);
			return this;
		}
		
		public PredicateObjectMapImpl build() {
			return new PredicateObjectMapImpl(
				predicateMaps,
				objectMaps
			);
		}
	}
}
