package com.taxonic.carml.rdf_mapper.annotations;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.taxonic.carml.rdf_mapper.PropertyHandler;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(RdfProperties.class)
public @interface RdfProperty {

	String value();

	boolean deprecated() default false;

	Class<? extends PropertyHandler> handler() default PropertyHandler.class;

}
