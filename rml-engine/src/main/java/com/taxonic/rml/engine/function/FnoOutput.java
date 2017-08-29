package com.taxonic.rml.engine.function;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface FnoOutput {

	String value();
	
}
