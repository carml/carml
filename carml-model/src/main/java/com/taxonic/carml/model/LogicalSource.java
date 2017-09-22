package com.taxonic.carml.model;

import org.eclipse.rdf4j.model.IRI;

public interface LogicalSource {

	Object getSource();
	
	String getIterator();
	
	IRI getReferenceFormulation();
	
}
