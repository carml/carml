package com.taxonic.rml.rdf_mapper;

import java.lang.reflect.Type;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public interface TypeDecider {

	Type decide(Model model, Resource resource);
	
}
