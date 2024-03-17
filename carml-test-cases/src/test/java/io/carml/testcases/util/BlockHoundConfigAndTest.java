package io.carml.testcases.util;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.auto.service.AutoService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.blockhound.integration.BlockHoundIntegration;
import reactor.core.scheduler.Schedulers;

class BlockHoundConfigAndTest {

    @AutoService(BlockHoundIntegration.class)
    public static final class BlockHoundConfig implements BlockHoundIntegration {

        @Override
        public void applyTo(BlockHound.Builder builder) {
            builder.allowBlockingCallsInside("io.netty.util.concurrent.FastThreadLocalRunnable", "run");
        }
    }

    @SuppressWarnings("java:S2925")
    @Test
    void blockHoundWorks() {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            assertThat("should fail", false);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            assertThat("detected", e.getCause() instanceof BlockingOperationError);
        }
    }
}
