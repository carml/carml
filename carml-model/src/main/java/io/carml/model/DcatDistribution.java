package io.carml.model;

import org.eclipse.rdf4j.model.IRI;

public interface DcatDistribution extends Source {

    IRI getAccessUrl();

    IRI getDownloadUrl();
}
