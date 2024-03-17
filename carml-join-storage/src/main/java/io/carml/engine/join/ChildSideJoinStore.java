package io.carml.engine.join;

import java.io.Serializable;
import java.util.Set;
import reactor.core.publisher.Flux;

public interface ChildSideJoinStore<T1 extends Serializable, T2 extends Serializable> {

    void addAll(Set<ChildSideJoin<T1, T2>> childSideJoins);

    Flux<ChildSideJoin<T1, T2>> clearingFlux();
}
