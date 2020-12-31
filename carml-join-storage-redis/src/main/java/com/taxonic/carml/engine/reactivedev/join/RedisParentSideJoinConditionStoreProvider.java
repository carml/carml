package com.taxonic.carml.engine.reactivedev.join;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import lombok.AllArgsConstructor;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

@AllArgsConstructor
public class RedisParentSideJoinConditionStoreProvider<T> implements ParentSideJoinConditionStoreProvider<T> {

  private final RedissonClient redisson;

  public static <T> RedisParentSideJoinConditionStoreProvider<T> getInstance(Config config) {
    return new RedisParentSideJoinConditionStoreProvider<>(Redisson.create(config));
  }

  @Override
  public ConcurrentMap<ParentSideJoinKey, Set<T>> create(String name) {
    return redisson.getMap(name);
  }
}
