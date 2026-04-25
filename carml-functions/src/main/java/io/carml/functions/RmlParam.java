package io.carml.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a method parameter as an RML function parameter, identified by its IRI. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface RmlParam {

    /** The IRI identifying this parameter. */
    String value();
}
