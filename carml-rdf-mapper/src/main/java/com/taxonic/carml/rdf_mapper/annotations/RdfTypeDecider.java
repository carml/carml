package com.taxonic.carml.rdf_mapper.annotations;

import com.taxonic.carml.rdf_mapper.TypeDecider;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface RdfTypeDecider {
  Class<? extends TypeDecider> value();
}
