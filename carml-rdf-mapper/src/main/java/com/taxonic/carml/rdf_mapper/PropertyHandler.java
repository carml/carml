package com.taxonic.carml.rdf_mapper;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public interface PropertyHandler {
	
	void handle(Model model, Resource resource, Object instance);

	boolean hasEffect(Model model, Resource resource);
}
