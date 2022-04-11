package com.taxonic.carml.engine;

import com.taxonic.carml.model.RefObjectMap;
import com.taxonic.carml.model.TriplesMap;
import reactor.core.publisher.Flux;

public interface RefObjectMapper<V> {

  TriplesMap getTriplesMap();

  RefObjectMap getRefObjectMap();

  Flux<V> resolveJoins(TriplesMapper<V> parentTriplesMapper);
}
