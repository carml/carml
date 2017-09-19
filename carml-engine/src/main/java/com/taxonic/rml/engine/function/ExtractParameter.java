package com.taxonic.rml.engine.function;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

interface ExtractParameter {
	
	Object extract(Model model, Resource subject);
	
}