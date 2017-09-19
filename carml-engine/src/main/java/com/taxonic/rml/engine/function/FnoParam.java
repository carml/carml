package com.taxonic.rml.engine.function;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface FnoParam {

	String value();

	// TODO boolean optional(); ?
	
	// TODO Class<? extends ValueAdapter> adapter(); ?
	
}
