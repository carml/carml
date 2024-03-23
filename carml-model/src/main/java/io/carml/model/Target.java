package io.carml.model;

import org.eclipse.rdf4j.model.IRI;

public interface Target extends Resource {

    IRI getSerialization();

    IRI getEncoding();

    IRI getCompression();
}
