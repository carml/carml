package com.taxonic.carml.engine.reactivedev.join;

import java.io.Serializable;
import java.util.HashSet;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Getter
@EqualsAndHashCode
public class ChildSideJoin<T1 extends Serializable, T2 extends Serializable> implements Serializable {

  private static final long serialVersionUID = 5242886114029652320L;

  private final HashSet<T1> subjects;

  private final HashSet<T2> predicates;

  private final HashSet<T1> graphs;

  @SuppressWarnings("java:S1948") // Suppressing this seemingly false positive warning
  private final HashSet<ChildSideJoinCondition> childSideJoinConditions;

}
