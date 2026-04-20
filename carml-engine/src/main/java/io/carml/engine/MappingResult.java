package io.carml.engine;

import io.carml.model.LogicalTarget;
import java.util.Set;
import org.reactivestreams.Publisher;

public interface MappingResult<T> {

    Set<LogicalTarget> getLogicalTargets();

    Publisher<T> getResults();
}
