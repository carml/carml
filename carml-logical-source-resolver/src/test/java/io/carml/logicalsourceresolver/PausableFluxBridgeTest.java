package io.carml.logicalsourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class PausableFluxBridgeTest {

    @Test
    void givenSource_whenAllItemsEmitted_thenFluxCompletesWithAllItems() {
        // Given
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new StubPausableSource(() -> {
                    for (int i = 0; i < 5; i++) {
                        emitter.next(i);
                    }
                    emitter.complete();
                }))
                .flux();

        // When / Then — StepVerifier.create(flux) sends request(Long.MAX_VALUE),
        // exercising the addCapped overflow guard in the onRequest handler.
        StepVerifier.create(flux).expectNext(0, 1, 2, 3, 4).verifyComplete();
    }

    @Test
    void givenSource_whenBackpressureApplied_thenSourceIsPaused() {
        // Given
        var pauseCount = new AtomicInteger();

        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> {
                    var source = new StubPausableSource(() -> {
                        for (int i = 0; i < 5; i++) {
                            emitter.next(i);
                        }
                        emitter.complete();
                    });
                    source.onPause = pauseCount::incrementAndGet;
                    return source;
                })
                .flux();

        // When / Then - request 2 at a time
        StepVerifier.create(flux, 2)
                .expectNext(0, 1)
                .thenAwait(Duration.ofMillis(50))
                .thenRequest(2)
                .expectNext(2, 3)
                .thenAwait(Duration.ofMillis(50))
                .thenRequest(1)
                .expectNext(4)
                .verifyComplete();

        assertThat(pauseCount.get() > 0, is(true));
    }

    @Test
    void givenSource_whenSlowSubscriber_thenAllItemsEventuallyDelivered() {
        // Given
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new StubPausableSource(() -> {
                    for (int i = 0; i < 4; i++) {
                        emitter.next(i);
                    }
                    emitter.complete();
                }))
                .flux();

        // When / Then - request one at a time
        StepVerifier.create(flux, 1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(50))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(50))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(50))
                .thenRequest(1)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void givenSourceThatThrowsOnStart_whenSubscribed_thenErrorPropagated() {
        // Given
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new StubPausableSource(() -> {
                    throw new RuntimeException("start failed");
                }))
                .flux();

        // When / Then
        StepVerifier.create(flux)
                .expectErrorMatches(
                        t -> t instanceof RuntimeException && t.getMessage().equals("start failed"))
                .verify();
    }

    @Test
    void givenSourceThatThrowsOnResume_whenDemandArrives_thenErrorPropagated() {
        // Given — source manually marks itself paused after emitting, so resume() is called on next request
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new PausableSource() {
                    volatile boolean paused;

                    @Override
                    public void start() {
                        emitter.next(1);
                        paused = true;
                    }

                    @Override
                    public void pause() {
                        paused = true;
                    }

                    @Override
                    public void resume() {
                        throw new RuntimeException("resume failed");
                    }

                    @Override
                    public boolean isPaused() {
                        return paused;
                    }

                    @Override
                    public boolean isCompleted() {
                        return false;
                    }
                })
                .flux();

        // When / Then
        StepVerifier.create(flux, 1)
                .expectNext(1)
                .thenAwait(Duration.ofMillis(50))
                .thenRequest(1)
                .expectErrorMatches(
                        t -> t instanceof RuntimeException && t.getMessage().equals("resume failed"))
                .verify();
    }

    @Test
    void givenDispose_whenSubscriptionCancelled_thenCleanupInvoked() throws InterruptedException {
        // Given
        var disposed = new CountDownLatch(1);

        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new StubPausableSource(() -> {
                    emitter.next(1);
                    emitter.complete();
                }))
                .onDispose(disposed::countDown)
                .flux();

        // When
        StepVerifier.create(flux).expectNext(1).verifyComplete();

        // Then
        assertThat(disposed.await(1, TimeUnit.SECONDS), is(true));
    }

    @Test
    void givenSourceCompletesBeforeDemandExhausted_thenFluxCompletes() {
        // Given - request 10 but only 3 items
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new StubPausableSource(() -> {
                    emitter.next(1);
                    emitter.next(2);
                    emitter.next(3);
                    emitter.complete();
                }))
                .flux();

        // When / Then
        StepVerifier.create(flux, 10).expectNext(1, 2, 3).verifyComplete();
    }

    @Test
    void givenResumableSource_whenBackpressureApplied_thenStartReturnsAndResumeResumes() {
        // Given - simulates the read loop pattern: start() returns when paused, resume() continues
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new ResumableSource(emitter, 6))
                .flux();

        // When / Then
        StepVerifier.create(flux, 2)
                .expectNext(0, 1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(2)
                .expectNext(2, 3)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(2)
                .expectNext(4, 5)
                .verifyComplete();
    }

    @Test
    void givenNoSourceFactory_whenBuild_thenThrowIllegalStateException() {
        // Given
        var builder = PausableFluxBridge.<Integer>builder();

        // When / Then
        var exception = assertThrows(IllegalStateException.class, builder::flux);
        assertThat(exception.getMessage(), is("sourceFactory must be set"));
    }

    @Test
    void givenEmitterError_whenCalled_thenFluxErrors() {
        // Given
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new StubPausableSource(() -> {
                    emitter.next(1);
                    emitter.error(new LogicalSourceResolverException("test error"));
                }))
                .flux();

        // When / Then
        StepVerifier.create(flux)
                .expectNext(1)
                .expectErrorMatches(t -> t instanceof LogicalSourceResolverException
                        && t.getMessage().equals("test error"))
                .verify();
    }

    @Test
    void givenSourceBlockedDuringStart_whenDemandArrivesBeforeStartReturns_thenPostStartReCheckResumes()
            throws InterruptedException {
        // Given — source emits 1 item in start(), manually pauses, then blocks on a latch.
        // The test thread issues request() on the subscription while start() is still blocked.
        // That onRequest sees startCompleted=false and skips resume. The test then releases
        // the latch, start() returns, and the post-start re-check must resume the source.
        var startBlocked = new CountDownLatch(1);
        var allowStartReturn = new CountDownLatch(1);
        var resumed = new AtomicBoolean(false);
        var receivedItems = new CopyOnWriteArrayList<Integer>();
        var completed = new CountDownLatch(1);

        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new PausableSource() {
                    volatile boolean paused;
                    volatile boolean done;

                    @Override
                    public void start() {
                        emitter.next(1);
                        paused = true;
                        startBlocked.countDown();
                        try {
                            if (!allowStartReturn.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Timed out waiting for allowStartReturn");
                            }
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    @Override
                    public void pause() {
                        paused = true;
                    }

                    @Override
                    public void resume() {
                        paused = false;
                        resumed.set(true);
                        emitter.next(2);
                        done = true;
                        emitter.complete();
                    }

                    @Override
                    public boolean isPaused() {
                        return paused;
                    }

                    @Override
                    public boolean isCompleted() {
                        return done;
                    }
                })
                .flux();

        // When — subscribe on a separate thread. subscribe() issues request(Long.MAX_VALUE),
        // which fires onRequest during registration. At that point startCompleted is false,
        // so onRequest skips resume. Then start() emits 1 item, pauses, and blocks.
        new Thread(() -> flux.subscribe(receivedItems::add, err -> completed.countDown(), completed::countDown))
                .start();

        // Wait until start() is blocked
        assertThat(startBlocked.await(5, TimeUnit.SECONDS), is(true));

        // Release start() — it returns, startCompleted becomes true, and the post-start
        // re-check sees isPaused=true + outstanding >= 0 → calls resume().
        allowStartReturn.countDown();

        // Then
        assertThat(completed.await(5, TimeUnit.SECONDS), is(true));
        assertThat(resumed.get(), is(true));
        assertThat(receivedItems, is(List.of(1, 2)));
    }

    @Test
    void givenCompletedSource_whenNewDemandArrives_thenSourceIsNotResumed() {
        // Given — source completes during start(), then subscriber requests more
        var resumeCount = new AtomicInteger();

        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new PausableSource() {
                    volatile boolean paused;
                    volatile boolean completed;

                    @Override
                    public void start() {
                        emitter.next(1);
                        completed = true;
                        emitter.complete();
                    }

                    @Override
                    public void pause() {
                        paused = true;
                    }

                    @Override
                    public void resume() {
                        resumeCount.incrementAndGet();
                    }

                    @Override
                    public boolean isPaused() {
                        return paused;
                    }

                    @Override
                    public boolean isCompleted() {
                        return completed;
                    }
                })
                .flux();

        // When / Then — request more after source has already completed
        StepVerifier.create(flux, 1).expectNext(1).thenRequest(5).verifyComplete();

        assertThat(resumeCount.get(), is(0));
    }

    @Test
    void givenEmitterNextCalledDuringSourceFactory_whenSourceRefNotYetSet_thenNoPauseAndItemDelivered() {
        // Given — sourceFactory calls emitter.next() before returning the source,
        // so sourceRef is still null. The null guard in Emitter.next() must prevent NPE.
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> {
                    emitter.next(42);
                    return new StubPausableSource(emitter::complete);
                })
                .flux();

        // When / Then
        StepVerifier.create(flux).expectNext(42).verifyComplete();
    }

    @Test
    void givenParkBasedSource_whenUnboundedDemand_thenAllItemsDelivered() {
        // Given — park-based source with unlimited demand (Long.MAX_VALUE from collectList).
        // The source never pauses because demand is always positive.
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new ParkBasedSource(emitter, 100))
                .flux();

        // When / Then
        StepVerifier.create(flux).expectNextCount(100).verifyComplete();
    }

    @Test
    void givenParkBasedSource_whenSubscribeOnDifferentThread_thenAllItemsDelivered() {
        // Given — park-based source with subscribeOn (matching the real DuckDB evaluator pattern).
        // No limitRate — the downstream requests Long.MAX_VALUE, so the source never pauses.
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new ParkBasedSource(emitter, 1000))
                .flux()
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());

        // When / Then
        StepVerifier.create(flux).expectNextCount(1000).expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void givenParkBasedSource_whenFullPipelineWithFlatMap_thenAllItemsDelivered() {
        // Given — full pattern matching the byte pipeline: limitRate + subscribeOn + flatMap
        // (with Flux.fromIterable) + publishOn. This works because flatMap creates async inner
        // publishers, so sink.next() returns immediately without blocking the producer thread.
        // (flatMapIterable would deadlock here because it blocks sink.next() synchronously.)
        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> new ParkBasedSource(emitter, 500))
                .flux()
                .limitRate(64)
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .flatMap(i -> reactor.core.publisher.Flux.fromIterable(List.of(i * 2, i * 2 + 1)))
                .publishOn(reactor.core.scheduler.Schedulers.boundedElastic());

        // When / Then — 500 items expanded to 1000
        var items = flux.collectList().block(Duration.ofSeconds(5));
        assertNotNull(items);
        assertThat(items.size(), is(1000));
    }

    @Test
    void givenParkBasedSource_whenCancelledAfterFewItems_thenProducerThreadExitsCleanly() throws InterruptedException {
        // Given — park-based source emitting many items; subscriber requests a few, then cancels.
        // Verify the producer thread exits cleanly (does not hang). Uses onDispose to cancel
        // the source, matching the real DuckDB evaluator pattern.
        var producerFinished = new CountDownLatch(1);
        var sourceRef = new AtomicReference<ParkBasedSource>();
        var itemCount = 10_000;

        var flux = PausableFluxBridge.<Integer>builder()
                .sourceFactory(emitter -> {
                    var source = new ParkBasedSource(emitter, itemCount) {
                        @Override
                        public void start() {
                            try {
                                super.start();
                            } finally {
                                producerFinished.countDown();
                            }
                        }
                    };
                    sourceRef.set(source);
                    return source;
                })
                .onDispose(() -> {
                    var source = sourceRef.get();
                    if (source != null) {
                        source.cancelSource();
                    }
                })
                .flux()
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());

        // When — request 3 items then cancel
        StepVerifier.create(flux, 3).expectNextCount(3).thenCancel().verify(Duration.ofSeconds(5));

        // Then — producer thread must exit within a reasonable time
        assertThat(producerFinished.await(5, TimeUnit.SECONDS), is(true));
    }

    /**
     * A source that parks the producer thread when demand is exhausted, similar to DuckDB's
     * evaluator pattern. Unlike {@link ResumableSource} which returns from start() on pause,
     * this source keeps start() running and parks the thread via {@link java.util.concurrent.locks.LockSupport#park}.
     */
    private static class ParkBasedSource implements PausableSource {
        private final PausableFluxBridge.Emitter<Integer> emitter;
        private final int itemCount;
        private volatile boolean paused;
        private volatile boolean completed;
        private volatile boolean cancelled;
        private volatile Thread producerThread;

        ParkBasedSource(PausableFluxBridge.Emitter<Integer> emitter, int itemCount) {
            this.emitter = emitter;
            this.itemCount = itemCount;
        }

        @Override
        public void start() {
            producerThread = Thread.currentThread();
            for (int i = 0; i < itemCount; i++) {
                if (cancelled) {
                    break;
                }
                emitter.next(i);
                // Park when demand is exhausted (pause was called by emitter.next)
                while (paused && !cancelled) {
                    java.util.concurrent.locks.LockSupport.park(this);
                }
            }
            completed = true;
            emitter.complete();
        }

        @Override
        public void pause() {
            paused = true;
        }

        @Override
        public void resume() {
            paused = false;
            var t = producerThread;
            if (t != null) {
                java.util.concurrent.locks.LockSupport.unpark(t);
            }
        }

        @Override
        public boolean isPaused() {
            return paused;
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        /** Cancels the source, unparking the producer thread so it exits its loop. */
        void cancelSource() {
            cancelled = true;
            var t = producerThread;
            if (t != null) {
                java.util.concurrent.locks.LockSupport.unpark(t);
            }
        }
    }

    /**
     * Simple stub source that runs a start action synchronously. Pause/resume are tracked via flags.
     */
    private static class StubPausableSource implements PausableSource {
        private final Runnable startAction;
        private volatile boolean paused;
        Runnable onPause;

        StubPausableSource(Runnable startAction) {
            this.startAction = startAction;
        }

        @Override
        public void start() {
            startAction.run();
        }

        @Override
        public void pause() {
            paused = true;
            if (onPause != null) {
                onPause.run();
            }
        }

        @Override
        public void resume() {
            paused = false;
        }

        @Override
        public boolean isPaused() {
            return paused;
        }

        @Override
        public boolean isCompleted() {
            return false;
        }
    }

    /**
     * A source that emits items in a loop, returning when paused. {@code resume()} re-enters the loop
     * from where it left off. Simulates the read loop pattern used by XPath and JSONPath resolvers.
     */
    private static class ResumableSource implements PausableSource {
        private final PausableFluxBridge.Emitter<Integer> emitter;
        private final int itemCount;
        private volatile boolean paused;
        private volatile boolean completed;
        private final AtomicInteger nextItem = new AtomicInteger();

        ResumableSource(PausableFluxBridge.Emitter<Integer> emitter, int itemCount) {
            this.emitter = emitter;
            this.itemCount = itemCount;
        }

        @Override
        public void start() {
            emitLoop();
        }

        @Override
        public void pause() {
            paused = true;
        }

        @Override
        public void resume() {
            paused = false;
            emitLoop();
        }

        @Override
        public boolean isPaused() {
            return paused;
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        private void emitLoop() {
            while (!completed && !paused && nextItem.get() < itemCount) {
                emitter.next(nextItem.getAndIncrement());
            }
            if (nextItem.get() >= itemCount && !completed) {
                completed = true;
                emitter.complete();
            }
        }
    }
}
