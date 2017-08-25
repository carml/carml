package com.taxonic.rml.engine.function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public interface ExecuteFunction {
	
	IRI getIri();
	
	Object execute(Model model, Resource subject);
	
}