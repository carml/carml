package com.taxonic.carml.model.impl;

import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import com.google.common.collect.ImmutableSet;
import com.taxonic.carml.rdf_mapper.TypeDecider;
import com.taxonic.carml.vocab.Rdf.Rr;
import com.taxonic.carml.vocab.Rdf.Carml;

public class ObjectMapTypeDecider implements TypeDecider {

	@Override
	public Set<Type> decide(Model model, Resource resource) {
		if (model.contains(resource, Rr.parentTriplesMap, null)) {
			if (model.contains(resource, Carml.multiJoinCondition, null)) {
				return ImmutableSet.of(CarmlMultiRefObjectMap.class);
			} else {
				return ImmutableSet.of(CarmlRefObjectMap.class);
			}
		} else if (isMultiObjectMap(model, resource)) {
			return ImmutableSet.of(CarmlMultiObjectMap.class);
		}
		
		return ImmutableSet.of(CarmlObjectMap.class);
	}
	
	private boolean isMultiObjectMap(Model model, Resource resource) {
		return 
				model.contains(resource, Carml.multiReference, null) ||
				model.contains(resource, Carml.multiTemplate, null) ||
				model.contains(resource, Carml.multiFunctionValue, null);
	}
	
}
