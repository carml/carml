package com.taxonic.carml.model;

import org.eclipse.rdf4j.model.IRI;

public interface LogicalSource {

	String getSource();
	
	String getIterator();
	
	IRI getReferenceFormulation();
	
}
