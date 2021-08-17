package com.taxonic.carml.engine.reactivedev.join;

import java.io.Serializable;
import java.util.ArrayList;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode
public class ChildSideJoinCondition implements Serializable {

  private static final long serialVersionUID = -3366382556631470961L;

  @NonNull
  String childReference;

  ArrayList<String> childValues;

  @NonNull
  String parentReference;

  @SuppressWarnings("java:S1319")
  public static ChildSideJoinCondition of(String childReference, ArrayList<String> childValues,
      String parentReference) {
    return new ChildSideJoinCondition(childReference, childValues, parentReference);
  }
}
