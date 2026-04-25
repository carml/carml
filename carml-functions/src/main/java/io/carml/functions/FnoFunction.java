package io.carml.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as an FnO function. Use {@link RmlFunction} instead.
 *
 * @deprecated Use {@link RmlFunction} instead.
 */
@Deprecated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface FnoFunction {

    String value();
}
