package io.carml.logicalsourceresolver;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Bridges a push-based {@link PausableSource} to a backpressure-aware {@link Flux} using
 * {@link Flux#create(Consumer) Flux.create()}.
 *
 * <p>Tracks outstanding demand via an {@link AtomicLong} counter that may go negative (indicating
 * items emitted beyond subscriber demand — buffered by {@code Flux.create()}). When a subscriber
 * requests items, the counter is incremented. Each item emitted via {@link Emitter#next(Object)}
 * decrements the counter. When the counter drops below zero, the source is automatically paused.
 * When new demand arrives and the source is paused and not completed, the source is resumed.
 *
 * <p><b>Threading model:</b> {@code start()} runs on the subscribing thread. {@code pause()} is
 * called synchronously from {@code Emitter.next()} on the same thread as the emitting code.
 * {@code resume()} is called from the Reactor request thread (the thread calling
 * {@code Subscription.request()}), which may differ from the start/emit thread. {@code resume()} is
 * only called after {@code start()} has returned, preventing concurrent start/resume overlap.
 *
 * @param <T> the element type emitted by the source
 */
public class PausableFluxBridge<T> {

    /**
     * Wraps a {@link FluxSink} with demand tracking. Each call to {@link #next(Object)} emits to the
     * sink, decrements the outstanding demand counter, and automatically pauses the source when demand
     * is exhausted.
     *
     * @param <T> the element type
     */
    public static class Emitter<T> {
        private final FluxSink<T> sink;
        private final AtomicLong outstandingRequests;
        private final AtomicReference<PausableSource> sourceRef;

        Emitter(FluxSink<T> sink, AtomicLong outstandingRequests) {
            this.sink = sink;
            this.outstandingRequests = outstandingRequests;
            this.sourceRef = new AtomicReference<>();
        }

        void setSource(PausableSource source) {
            this.sourceRef.set(source);
        }

        /**
         * Emits an item to the subscriber and decrements the demand counter. If outstanding demand
         * drops below zero, the source is automatically paused.
         */
        public void next(T item) {
            sink.next(item);
            var outstanding = outstandingRequests.decrementAndGet();
            var source = sourceRef.get();
            if (outstanding < 0L && source != null && !source.isPaused()) {
                source.pause();
            }
        }

        /** Signals completion to the subscriber. */
        public void complete() {
            sink.complete();
        }

        /** Signals an error to the subscriber. */
        public void error(Throwable throwable) {
            sink.error(throwable);
        }
    }

    private final Function<Emitter<T>, PausableSource> sourceFactory;
    private final Runnable onDispose;

    private PausableFluxBridge(Function<Emitter<T>, PausableSource> sourceFactory, Runnable onDispose) {
        this.sourceFactory = sourceFactory;
        this.onDispose = onDispose;
    }

    /**
     * Creates a new builder.
     *
     * @param <T> the element type
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Returns a backpressure-aware {@link Flux} that bridges the push-based source. */
    public Flux<T> flux() {
        return Flux.create(this::bridgeSource);
    }

    private void bridgeSource(FluxSink<T> sink) {
        var outstandingRequests = new AtomicLong();
        var startCompleted = new AtomicBoolean(false);
        var emitter = new Emitter<>(sink, outstandingRequests);

        var source = sourceFactory.apply(emitter);
        emitter.setSource(source);

        // Reactor's Flux.create() serializes onRequest signals, so concurrent resume
        // is not possible under normal usage.
        sink.onRequest(requested -> {
            var outstanding = addCapped(outstandingRequests, requested);
            if (startCompleted.get() && shouldResume(source, outstanding)) {
                resumeSource(source, sink);
            }
        });

        if (onDispose != null) {
            sink.onDispose(onDispose::run);
        }

        startSource(source, sink, startCompleted);

        // Re-check: demand may have arrived via onRequest while start() was running.
        // Those onRequest calls skipped resume because startCompleted was still false.
        if (shouldResume(source, outstandingRequests.get())) {
            resumeSource(source, sink);
        }
    }

    private static boolean shouldResume(PausableSource source, long outstanding) {
        return source.isPaused() && !source.isCompleted() && outstanding >= 0L;
    }

    private static void startSource(PausableSource source, FluxSink<?> sink, AtomicBoolean startCompleted) {
        try {
            source.start();
        } catch (Exception ex) {
            sink.error(ex);
        } finally {
            startCompleted.set(true);
        }
    }

    private static void resumeSource(PausableSource source, FluxSink<?> sink) {
        try {
            source.resume();
        } catch (Exception ex) {
            sink.error(ex);
        }
    }

    /** Adds {@code requested} to the counter, capping at {@code Long.MAX_VALUE} to prevent overflow. */
    private static long addCapped(AtomicLong counter, long requested) {
        return counter.accumulateAndGet(requested, (current, add) -> {
            var result = current + add;
            return result < 0 ? Long.MAX_VALUE : result;
        });
    }

    public static class Builder<T> {
        private Function<Emitter<T>, PausableSource> sourceFactory;
        private Runnable onDispose;

        /**
         * Sets the factory that creates a {@link PausableSource} given an {@link Emitter}. The source
         * should use the emitter to push items to the Flux.
         */
        public Builder<T> sourceFactory(Function<Emitter<T>, PausableSource> sourceFactory) {
            this.sourceFactory = sourceFactory;
            return this;
        }

        /** Sets a callback invoked when the subscriber disposes the subscription. */
        public Builder<T> onDispose(Runnable onDispose) {
            this.onDispose = onDispose;
            return this;
        }

        /** Builds the bridge and returns the backpressure-aware {@link Flux}. */
        public Flux<T> flux() {
            if (sourceFactory == null) {
                throw new IllegalStateException("sourceFactory must be set");
            }
            return new PausableFluxBridge<>(sourceFactory, onDispose).flux();
        }
    }
}
