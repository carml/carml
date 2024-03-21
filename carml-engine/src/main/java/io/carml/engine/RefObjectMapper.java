package io.carml.engine;

import io.carml.model.RefObjectMap;
import io.carml.model.TriplesMap;
import reactor.core.publisher.Flux;

public interface RefObjectMapper<V> {

    TriplesMap getTriplesMap();

    RefObjectMap getRefObjectMap();

    Flux<MappingResult<V>> resolveJoins(TriplesMapper<V> parentTriplesMapper);
}
