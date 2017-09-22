package com.taxonic.carml.engine;

import java.io.InputStream;
import java.util.Optional;
import java.util.function.Function;

public interface SourceResolver extends Function<Object, Optional<InputStream>> {

}
