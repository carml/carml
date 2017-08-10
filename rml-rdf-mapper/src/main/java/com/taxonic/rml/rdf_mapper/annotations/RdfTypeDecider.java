package com.taxonic.rml.rdf_mapper.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.taxonic.rml.rdf_mapper.TypeDecider;

@Retention(RetentionPolicy.RUNTIME)
public @interface RdfTypeDecider {
	Class<? extends TypeDecider> value();
}
