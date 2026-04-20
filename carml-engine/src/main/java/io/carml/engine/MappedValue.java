package io.carml.engine;

import io.carml.model.LogicalTarget;
import java.util.Set;

public interface MappedValue<T> {

    T getValue();

    Set<LogicalTarget> getLogicalTargets();
}
