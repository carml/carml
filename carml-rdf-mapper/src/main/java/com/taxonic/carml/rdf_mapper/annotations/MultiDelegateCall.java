package com.taxonic.carml.rdf_mapper.annotations;

import com.taxonic.carml.rdf_mapper.Combiner;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface MultiDelegateCall {

    Class<? extends Combiner> value() default DEFAULT.class;

    static abstract class DEFAULT implements Combiner {}

}
