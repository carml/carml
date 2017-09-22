package com.taxonic.carml.rdf_mapper;

import java.lang.reflect.Type;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public interface Mapper {

	<T> T map(Model model, Resource resource, Type type);
	
	Type getType(IRI rdfType);

	void addType(IRI rdfType, Type type);
	
}
