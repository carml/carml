package com.taxonic.rml.model;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

public interface TermMap {

	String getReference();
	
	String getInverseExpression();
	
	String getTemplate();
	
	IRI getTermType();

	Value getConstant();
	
}
