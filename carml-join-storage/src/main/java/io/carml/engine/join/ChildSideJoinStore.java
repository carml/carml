package io.carml.engine.join;

import java.util.Set;
import reactor.core.publisher.Flux;

public interface ChildSideJoinStore<T1, T2> {

    void addAll(Set<ChildSideJoin<T1, T2>> childSideJoins);

    Flux<ChildSideJoin<T1, T2>> clearingFlux();
}
