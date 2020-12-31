package com.taxonic.carml.engine.reactivedev.join;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MapDbParentSideJoinConditionStoreProvider<T> implements ParentSideJoinConditionStoreProvider<T> {

  private final DBMaker.Maker dbMaker;

  public static <T> MapDbParentSideJoinConditionStoreProvider<T> getInstance() {
    return new MapDbParentSideJoinConditionStoreProvider<>(DBMaker.memoryDB()
        .closeOnJvmShutdown());
  }

  @SuppressWarnings("unchecked")
  @Override
  public ConcurrentMap<ParentSideJoinKey, Set<T>> create(String name) {
    DB db = dbMaker.make();
    return db.<ParentSideJoinKey, Set<T>>hashMap(name, Serializer.JAVA, Serializer.JAVA)
        .create();
  }
}
