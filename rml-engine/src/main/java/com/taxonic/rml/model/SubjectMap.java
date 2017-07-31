package com.taxonic.rml.model;

import java.util.Set;

import org.eclipse.rdf4j.model.IRI;

public interface SubjectMap extends TermMap {

	Set<IRI> getClasses();
	
}
