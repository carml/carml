package io.carml.model;

import org.eclipse.rdf4j.model.IRI;

public interface Source extends Resource {

    IRI getEncoding();

    String getNull();

    IRI getCompression();
}
