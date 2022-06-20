package io.carml.engine.join.impl;

import io.carml.engine.join.ChildSideJoin;
import io.carml.engine.join.ChildSideJoinStore;
import java.io.Serializable;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import reactor.core.publisher.Flux;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CarmlChildSideJoinStore<T1 extends Serializable, T2 extends Serializable>
    implements ChildSideJoinStore<T1, T2> {

  private final String name;

  private final Set<ChildSideJoin<T1, T2>> storage;

  static <T1 extends Serializable, T2 extends Serializable> CarmlChildSideJoinStore<T1, T2> of(String name,
      Set<ChildSideJoin<T1, T2>> storage) {
    return new CarmlChildSideJoinStore<>(name, storage);
  }

  @Override
  public void addAll(Set<ChildSideJoin<T1, T2>> childSideJoins) {
    storage.addAll(childSideJoins);
  }

  @Override
  public Flux<ChildSideJoin<T1, T2>> clearingFlux() {
    return Flux.using(() -> storage, Flux::fromIterable, Set::clear);
  }
}
