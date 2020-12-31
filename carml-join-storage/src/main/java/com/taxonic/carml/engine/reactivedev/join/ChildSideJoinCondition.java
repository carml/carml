package com.taxonic.carml.engine.reactivedev.join;

import java.io.Serializable;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode
public class ChildSideJoinCondition implements Serializable {

  @NonNull
  String childReference;

  List<String> childValues;

  @NonNull
  String parentReference;

  public static ChildSideJoinCondition of(String childReference, List<String> childValues, String parentReference) {
    return new ChildSideJoinCondition(childReference, childValues, parentReference);
  }
}
