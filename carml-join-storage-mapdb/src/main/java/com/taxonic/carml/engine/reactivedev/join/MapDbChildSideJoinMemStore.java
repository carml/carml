package com.taxonic.carml.engine.reactivedev.join;

import java.io.Serializable;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import reactor.core.publisher.Flux;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MapDbChildSideJoinMemStore<T1 extends Serializable, T2 extends Serializable>
    implements ChildSideJoinStore<T1, T2> {

  String name;

  Set<ChildSideJoin<T1, T2>> childSideJoinStore;

  static <T1 extends Serializable, T2 extends Serializable> MapDbChildSideJoinMemStore<T1, T2> of(String name,
      Set<ChildSideJoin<T1, T2>> childSideJoinStore) {
    return new MapDbChildSideJoinMemStore<>(name, childSideJoinStore);
  }

  @Override
  public void addAll(Set<ChildSideJoin<T1, T2>> childSideJoins) {
    childSideJoinStore.addAll(childSideJoins);
  }

  @Override
  public Flux<ChildSideJoin<T1, T2>> clearingFlux() {
    return Flux.using(() -> childSideJoinStore, Flux::fromIterable, Set::clear);
  }
}
