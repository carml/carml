package io.carml.model;

import java.util.Set;
import org.eclipse.rdf4j.model.IRI;

public interface Source extends Resource {

    IRI getEncoding();

    Set<Object> getNulls();

    IRI getCompression();
}
