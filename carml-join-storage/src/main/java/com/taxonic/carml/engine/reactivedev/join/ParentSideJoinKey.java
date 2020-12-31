package com.taxonic.carml.engine.reactivedev.join;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode
public class ParentSideJoinKey implements Serializable {

  @NonNull
  String parentReference;

  @NonNull
  String parentValue;

  public static ParentSideJoinKey of(@NonNull String parentReference, @NonNull String parentValue) {
    return new ParentSideJoinKey(parentReference, parentValue);
  }

}
