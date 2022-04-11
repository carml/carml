package com.taxonic.carml.engine.join;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode
@ToString
public class ParentSideJoinKey implements Serializable {

  private static final long serialVersionUID = -8655379521016481101L;

  @NonNull
  String parentReference;

  @NonNull
  String parentValue;

  public static ParentSideJoinKey of(@NonNull String parentReference, @NonNull String parentValue) {
    return new ParentSideJoinKey(parentReference, parentValue);
  }

}
