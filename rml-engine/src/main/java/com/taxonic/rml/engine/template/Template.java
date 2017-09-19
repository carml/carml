package com.taxonic.rml.engine.template;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface Template {

	Set<Expression> getExpressions();
	
	Builder newBuilder();
	
	interface Builder {
		
		Builder bind(Expression expression, Function<Expression, Optional<String>> templateValue);
		
		Optional<Object> create();
		
	}
	
	interface Expression {
		
		String getValue();
		
	}
	
}
