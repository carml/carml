package com.taxonic.carml.model.impl;

import java.lang.reflect.Type;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

import com.taxonic.carml.rdf_mapper.TypeDecider;
import com.taxonic.carml.vocab.Rdf.Rr;

public class ObjectMapTypeDecider implements TypeDecider {

	@Override
	public Type decide(Model model, Resource resource) {
		if (model.contains(resource, Rr.parentTriplesMap, null))
			return RefObjectMapImpl.class;
		return ObjectMapImpl.class;
	}

}
