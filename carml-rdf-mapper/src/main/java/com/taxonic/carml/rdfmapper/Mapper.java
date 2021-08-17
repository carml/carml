package com.taxonic.carml.rdfmapper;

import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public interface Mapper {

  <T> T map(Model model, Resource resource, Set<Type> types);

  Type getDecidableType(IRI rdfType);

  void addDecidableType(IRI rdfType, Type type);

  void bindInterfaceImplementation(Type interfaze, Type implementation);

  Type getInterfaceImplementation(Type interfaze);
}
