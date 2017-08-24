package com.taxonic.rml.model;

import org.eclipse.rdf4j.model.Value;

public interface TermMap {

	String getReference();
	
	String getInverseExpression();
	
	String getTemplate();
	
	TermType getTermType();

	Value getConstant();
	
}
