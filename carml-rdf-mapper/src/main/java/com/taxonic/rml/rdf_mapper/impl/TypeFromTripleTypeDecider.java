package com.taxonic.rml.rdf_mapper.impl;

import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import com.taxonic.rml.rdf_mapper.TypeDecider;

class TypeFromTripleTypeDecider implements TypeDecider {

	private TypeDecider propertyTypeDecider;
	
	TypeFromTripleTypeDecider(TypeDecider propertyTypeDecider) {
		this.propertyTypeDecider = propertyTypeDecider;
	}

	@Override
	public Type decide(Model model, Resource resource) {
		
		List<IRI> rdfTypes =
			model.filter(resource, RDF.TYPE, null).objects().stream()
				.map(v -> (IRI) v)
				.collect(Collectors.toList());
		
		// TODO what if multiple rdf:types? probably choose the only 1 that's known/registered. what if multiple of those?
		if (rdfTypes.size() > 1)
			throw new RuntimeException("multiple rdf:type triples found for resource [" + resource + "]; can't handle that yet");
		
		// if no rdf:type, use property type (or its registered implementation) as target type
		if (rdfTypes.isEmpty())
			return propertyTypeDecider.decide(model, resource);
		
		IRI rdfType = rdfTypes.get(0);
		// TODO mapper.getJavaType(rdfType) : Type
		throw new RuntimeException("cannot resolve java type for rdf type [" + rdfType + "]");
		
	}

}
