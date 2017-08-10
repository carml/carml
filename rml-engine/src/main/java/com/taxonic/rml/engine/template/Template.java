package com.taxonic.rml.engine.template;

import java.util.Set;

public interface Template {

	Set<Expression> getExpressions();
	
	Builder newBuilder();
	
	interface Builder {
		
		Builder bind(Expression expression, String value);
		
		String create();
		
	}
	
	interface Expression {
		
		String getValue();
		
	}
	
}
