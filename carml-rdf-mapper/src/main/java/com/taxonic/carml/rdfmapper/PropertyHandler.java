package com.taxonic.carml.rdfmapper;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public interface PropertyHandler {

  void handle(Model model, Resource resource, Object instance);

  boolean hasEffect(Model model, Resource resource);
}
