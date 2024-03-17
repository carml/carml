package io.carml.engine.rdf;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public record RdfCollectionOrContainer(IRI type, Resource head, Model model) {}
