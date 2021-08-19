package com.taxonic.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.scheduler.Schedulers;

class RmlMapperTest {

  @SuppressWarnings("java:S2925")
  @Test
  void blockHoundWorks() {
    try {
      FutureTask<?> task = new FutureTask<>(() -> {
        Thread.sleep(0);
        return "";
      });
      Schedulers.parallel()
          .schedule(task);

      task.get(10, TimeUnit.SECONDS);
      assertThat("should fail", false);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      assertThat("detected", e.getCause() instanceof BlockingOperationError);
    }
  }

}
