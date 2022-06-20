package io.carml.rdfmapper.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface RdfType {

  Class<?> value();

  boolean deprecated() default false;

}
