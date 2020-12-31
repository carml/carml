package com.taxonic.carml.engine.reactivedev.join;

import java.util.Set;
import lombok.AllArgsConstructor;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

@AllArgsConstructor
public class RedisChildSideJoinStoreProvider<T1, T2> implements ChildSideJoinStoreProvider<T1, T2> {

  private final RedissonClient redisson;

  public static <T1, T2> RedisChildSideJoinStoreProvider<T1, T2> getInstance(Config config) {
    return new RedisChildSideJoinStoreProvider<>(Redisson.create(config));
  }

  @Override
  public Set<ChildSideJoin<T1, T2>> create(String name) {
    return redisson.getSet(name);
  }
}
