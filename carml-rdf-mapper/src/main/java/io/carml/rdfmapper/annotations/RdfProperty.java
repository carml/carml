package io.carml.rdfmapper.annotations;

import io.carml.rdfmapper.PropertyHandler;
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
