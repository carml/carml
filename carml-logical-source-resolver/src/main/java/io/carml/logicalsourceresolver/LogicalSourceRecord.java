package io.carml.logicalsourceresolver;

import io.carml.model.LogicalSource;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

@Data(staticConstructor = "of")
@Getter
public class LogicalSourceRecord<R> {

  @NonNull
  private LogicalSource logicalSource;

  @NonNull
  private R record;
}
