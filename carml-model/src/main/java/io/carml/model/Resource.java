package io.carml.model;

import io.carml.rdfmapper.annotations.MultiDelegateCall;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;

public interface Resource {

  String getId();

  String getLabel();

  Set<Resource> getReferencedResources();

  default String getResourceName() {
    return getLabel() != null ? "\"" + getLabel() + "\"" : getAsResource().toString();
  }

  org.eclipse.rdf4j.model.Resource getAsResource();

  @MultiDelegateCall
  void addTriples(ModelBuilder modelBuilder);

  @MultiDelegateCall(AsRdfCombiner.class)
  Model asRdf();
}
