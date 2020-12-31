package com.taxonic.carml.engine.reactivedev.join;

import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MapDbChildSideJoinStoreProvider<T1, T2> implements ChildSideJoinStoreProvider<T1, T2> {

  private final DBMaker.Maker dbMaker;

  public static <T1, T2> MapDbChildSideJoinStoreProvider<T1, T2> getInstance() {
    return new MapDbChildSideJoinStoreProvider<>(DBMaker.memoryDB()
        .closeOnJvmShutdown());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Set<ChildSideJoin<T1, T2>> create(String name) {
    DB db = dbMaker.make();
    return (Set<ChildSideJoin<T1, T2>>) db.hashSet(name, Serializer.JAVA)
        .create();
  }
}
