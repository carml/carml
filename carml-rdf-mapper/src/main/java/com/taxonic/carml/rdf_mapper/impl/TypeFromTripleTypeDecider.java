package com.taxonic.carml.rdf_mapper.impl;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import com.taxonic.carml.rdf_mapper.Mapper;
import com.taxonic.carml.rdf_mapper.TypeDecider;

public class TypeFromTripleTypeDecider implements TypeDecider {

	private Mapper mapper;
	private Optional<TypeDecider> propertyTypeDecider;
	
	public TypeFromTripleTypeDecider(Mapper mapper) {
		this(mapper, Optional.empty());
	}
	
	public TypeFromTripleTypeDecider(Mapper mapper, Optional<TypeDecider> propertyTypeDecider) {
		this.mapper = mapper;
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
		if (rdfTypes.isEmpty() && propertyTypeDecider.isPresent())
			return propertyTypeDecider.get().decide(model, resource);
		
		IRI rdfType = rdfTypes.get(0);
		return mapper.getType(rdfType);
	}

}
