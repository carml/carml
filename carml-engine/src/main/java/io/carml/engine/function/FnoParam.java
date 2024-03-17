package io.carml.engine.function;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@SuppressWarnings("java:S1135")
@Retention(RetentionPolicy.RUNTIME)
public @interface FnoParam {

    String value();

    // TODO boolean optional(); ?

    // TODO Class<? extends ValueAdapter> adapter(); ?

}
