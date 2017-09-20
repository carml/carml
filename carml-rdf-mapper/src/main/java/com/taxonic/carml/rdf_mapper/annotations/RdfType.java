package com.taxonic.carml.rdf_mapper.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface RdfType {

	Class<?> value();
	
}
