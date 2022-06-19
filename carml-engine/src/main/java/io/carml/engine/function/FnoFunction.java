package io.carml.engine.function;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@SuppressWarnings("java:S1135")
@Retention(RetentionPolicy.RUNTIME)
public @interface FnoFunction {

  String value();

  // TODO Class<? extends ReturnValueAdapter> adapter(); ?

}
