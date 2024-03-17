package io.carml.rdfmapper.annotations;

import io.carml.rdfmapper.Combiner;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@SuppressWarnings("rawtypes")
@Retention(RetentionPolicy.RUNTIME)
public @interface MultiDelegateCall {

    Class<? extends Combiner> value() default Default.class;

    interface Default extends Combiner {}
}
