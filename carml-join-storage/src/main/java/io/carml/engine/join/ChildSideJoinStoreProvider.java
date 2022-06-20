package io.carml.engine.join;

import java.io.Serializable;
import lombok.NonNull;

public interface ChildSideJoinStoreProvider<T1 extends Serializable, T2 extends Serializable> {

  ChildSideJoinStore<T1, T2> createChildSideJoinStore(@NonNull String name);

}
