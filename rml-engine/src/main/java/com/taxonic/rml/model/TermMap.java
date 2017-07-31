package com.taxonic.rml.model;

import org.eclipse.rdf4j.model.Resource;

public interface TermMap {

	String getReference();
	
	String getInverseExpression();
	
	String getTemplate();
	
	Object getTermType();

	Resource getConstant();
	
}
