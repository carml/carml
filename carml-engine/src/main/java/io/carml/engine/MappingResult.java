package io.carml.engine;

import io.carml.model.Target;
import java.util.Set;
import org.reactivestreams.Publisher;

public interface MappingResult<T> {

    Set<Target> getTargets();

    Publisher<T> getResults();
}
