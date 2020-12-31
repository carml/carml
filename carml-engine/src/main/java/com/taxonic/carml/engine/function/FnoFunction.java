package com.taxonic.carml.engine.function;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface FnoFunction {

  String value();

  // TODO Class<? extends ReturnValueAdapter> adapter(); ?

}
