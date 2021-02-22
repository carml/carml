package com.taxonic.carml.model.impl;

import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.rdf_mapper.TypeDecider;
import com.taxonic.carml.vocab.Rdf;
import com.taxonic.carml.vocab.Rdf.Rr;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

import java.lang.reflect.Type;
import java.util.Set;

public class ObjectMapTypeDecider implements TypeDecider {

	@Override
	public Set<Type> decide(Model model, Resource resource) {
		if (model.contains(resource, Rr.parentTriplesMap, null)) {
			return ImmutableSet.of(CarmlRefObjectMap.class);
		}
		if (model.contains(resource, Rdf.CarmlExp.subTriplesMap, null)) {
			return ImmutableSet.of(CarmlNestedMapping.class);
		}
		return ImmutableSet.of(CarmlObjectMap.class);
	}

}
