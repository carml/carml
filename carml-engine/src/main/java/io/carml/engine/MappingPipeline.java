package io.carml.engine;

import io.carml.logicalsourceresolver.LogicalSourceResolver;
import io.carml.model.Source;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor(staticName = "of")
@Getter
public class MappingPipeline<T> {

    @NonNull
    private Set<TriplesMapper<T>> triplesMappers;

    @NonNull
    private Map<RefObjectMapper<T>, TriplesMapper<T>> refObjectMapperToTriplesMapper;

    @NonNull
    private Map<Source, LogicalSourceResolver<?>> sourceToLogicalSourceResolver;
}
