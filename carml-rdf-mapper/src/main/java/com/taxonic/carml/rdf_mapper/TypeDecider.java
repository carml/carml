package com.taxonic.carml.rdf_mapper;

import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public interface TypeDecider {

  Set<Type> decide(Model model, Resource resource);

}
