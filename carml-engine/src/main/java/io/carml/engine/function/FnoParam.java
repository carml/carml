package io.carml.engine.function;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method parameter as an FnO function parameter. Use {@link RmlParam} instead.
 *
 * @deprecated Use {@link RmlParam} instead.
 */
@Deprecated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface FnoParam {

    String value();
}
