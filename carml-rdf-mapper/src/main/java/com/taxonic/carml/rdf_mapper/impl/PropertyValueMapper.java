package com.taxonic.carml.rdf_mapper.impl;

import java.util.List;
import java.util.Optional;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;

interface PropertyValueMapper {
	
	Optional<Object> map(Model model, Object instance, List<Value> values);
	
}