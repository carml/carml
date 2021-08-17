package com.taxonic.carml.engine.reactivedev.join;

import java.io.Serializable;
import lombok.NonNull;

public interface ParentSideJoinConditionStoreProvider<T extends Serializable> {

  ParentSideJoinConditionStore<T> createParentSideJoinConditionStore(@NonNull String name);

}
