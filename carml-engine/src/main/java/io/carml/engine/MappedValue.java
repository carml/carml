package io.carml.engine;

import io.carml.model.Target;
import java.util.Set;

public interface MappedValue<T> {

    T getValue();

    Set<Target> getTargets();
}
