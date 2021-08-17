package com.taxonic.carml.rdfmapper.impl;

import java.util.List;
import java.util.Optional;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

interface PropertyValueMapper {

  Optional<Object> map(Model model, Resource resource, Object instance, List<Value> values);

}
