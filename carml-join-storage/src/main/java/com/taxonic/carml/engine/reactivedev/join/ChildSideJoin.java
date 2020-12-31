package com.taxonic.carml.engine.reactivedev.join;

import java.io.Serializable;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Getter
@EqualsAndHashCode
public class ChildSideJoin<T1, T2> implements Serializable {

  private Set<T1> subjects;

  private Set<T2> predicates;

  private Set<T1> graphs;

  private Set<ChildSideJoinCondition> childSideJoinConditions;

}
