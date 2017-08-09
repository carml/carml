package com.taxonic.rml.engine.template;

import java.util.Set;

public interface Template {

	Set<String> getVariables();
	
	Builder newBuilder();
	
	interface Builder {
		
		Builder bind(String variable, Object value);
		
		String create();
		
	}
}
