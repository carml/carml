package io.carml.logicalsourceresolver;

import io.carml.model.LogicalSource;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor(staticName = "of")
@Getter
public class LogicalSourceRecord<R> {

  @NonNull
  private LogicalSource logicalSource;

  @NonNull
  private R sourceRecord;
}
