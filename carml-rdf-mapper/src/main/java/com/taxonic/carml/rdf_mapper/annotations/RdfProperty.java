package com.taxonic.carml.rdf_mapper.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.taxonic.carml.rdf_mapper.PropertyHandler;

@Retention(RetentionPolicy.RUNTIME)
public @interface RdfProperty {

	String value();
	
	Class<? extends PropertyHandler> handler() default PropertyHandler.class;
	
}
