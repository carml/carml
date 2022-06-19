package io.carml.model;

import org.eclipse.rdf4j.model.IRI;

public interface LogicalSource extends Resource {

  Object getSource();

  String getIterator();

  IRI getReferenceFormulation();

}
