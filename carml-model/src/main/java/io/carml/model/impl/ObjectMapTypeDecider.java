package io.carml.model.impl;

import io.carml.rdfmapper.TypeDecider;
import io.carml.vocab.Rdf.Rml;
import io.carml.vocab.Rdf.Rr;
import java.lang.reflect.Type;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public class ObjectMapTypeDecider implements TypeDecider {

  @Override
  public Set<Type> decide(Model model, Resource resource) {
    if (model.contains(resource, Rml.parentTriplesMap, null) || model.contains(resource, Rr.parentTriplesMap, null)) {
      return Set.of(CarmlRefObjectMap.class);
    }
    return Set.of(CarmlObjectMap.class);
  }
}
