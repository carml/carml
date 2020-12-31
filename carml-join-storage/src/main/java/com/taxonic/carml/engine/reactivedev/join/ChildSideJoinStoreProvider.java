package com.taxonic.carml.engine.reactivedev.join;

import java.util.Set;

public interface ChildSideJoinStoreProvider<T1, T2> {

  Set<ChildSideJoin<T1, T2>> create(String name);

}
