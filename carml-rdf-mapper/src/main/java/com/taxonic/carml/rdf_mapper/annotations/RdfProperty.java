package com.taxonic.carml.rdf_mapper.annotations;

import com.taxonic.carml.rdf_mapper.PropertyHandler;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(RdfProperties.class)
public @interface RdfProperty {

  String value();

  boolean deprecated() default false;

  Class<? extends PropertyHandler> handler() default PropertyHandler.class;

}
