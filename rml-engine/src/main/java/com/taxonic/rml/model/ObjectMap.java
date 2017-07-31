package com.taxonic.rml.model;

import org.eclipse.rdf4j.model.IRI;

public interface ObjectMap extends TermMap {

	IRI getDatatype();
	
	String getLanguage();
	
}
