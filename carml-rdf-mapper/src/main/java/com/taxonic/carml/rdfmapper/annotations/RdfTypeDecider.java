package com.taxonic.carml.rdfmapper.annotations;

import com.taxonic.carml.rdfmapper.TypeDecider;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface RdfTypeDecider {
  Class<? extends TypeDecider> value();
}
