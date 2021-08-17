package com.taxonic.carml.rdfmapper.annotations;

import com.taxonic.carml.rdfmapper.Combiner;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@SuppressWarnings("rawtypes")
@Retention(RetentionPolicy.RUNTIME)
public @interface MultiDelegateCall {

  Class<? extends Combiner> value() default Default.class;

  interface Default extends Combiner {
  }

}
