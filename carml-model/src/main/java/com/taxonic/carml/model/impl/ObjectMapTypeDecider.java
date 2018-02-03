package com.taxonic.carml.model.impl;

import java.lang.reflect.Type;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

import com.taxonic.carml.rdf_mapper.TypeDecider;
import com.taxonic.carml.vocab.Rdf.Rr;
import com.taxonic.carml.vocab.Rdf.Carml;

public class ObjectMapTypeDecider implements TypeDecider {

	@Override
	public Type decide(Model model, Resource resource) {
		if (model.contains(resource, Rr.parentTriplesMap, null)) {
			if (model.contains(resource, Carml.multiJoinCondition, null)) {
				return CarmlMultiRefObjectMap.class;
			} else {
				return CarmlRefObjectMap.class;
			}
		} else if (isMultiObjectMap(model, resource)) {
			return CarmlMultiObjectMap.class;
		}
		
		return CarmlObjectMap.class;
	}
	
	private boolean isMultiObjectMap(Model model, Resource resource) {
		return 
				model.contains(resource, Carml.multiReference, null) ||
				model.contains(resource, Carml.multiTemplate, null) ||
				model.contains(resource, Carml.multiFunctionValue, null);
	}
	
}
