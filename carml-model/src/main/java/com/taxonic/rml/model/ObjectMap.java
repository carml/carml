package com.taxonic.rml.model;

import org.eclipse.rdf4j.model.IRI;

public interface ObjectMap extends TermMap, BaseObjectMap {

	IRI getDatatype();
	
	String getLanguage();
	
}
