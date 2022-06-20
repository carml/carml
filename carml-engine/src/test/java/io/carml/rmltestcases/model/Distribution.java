package io.carml.rmltestcases.model;

import org.eclipse.rdf4j.model.IRI;

public interface Distribution extends Resource {

  IRI getDownloadUrl();

  String getRelativeFileLocation();

}
