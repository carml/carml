package io.carml.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import org.eclipse.rdf4j.model.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * Default implementation of {@link MappingExecution} that wraps a reactive statement pipeline with
 * lifecycle management. Tracks statement and error counts via atomic counters, supports
 * cancellation via a Reactor {@link Sinks.Empty} signal, and provides checkpoint support for
 * observer notification.
 *
 * <p>Thread-safe: all mutable state uses atomic or volatile access. The {@link #statements()} flux
 * is intended for single subscription — subscribing multiple times will accumulate counters across
 * all subscriptions.
 */
class DefaultMappingExecution implements MappingExecution {

    private final Flux<Statement> statements;
    private final Sinks.Empty<Void> cancelSink;
    private final MappingExecutionObserver observer;
    private final List<ResolvedMapping> resolvedMappings;

    private final AtomicLong statementsProduced = new AtomicLong();
    private final AtomicLong errorsEncountered = new AtomicLong();
    private final Instant startedAt;

    // Checkpoint tracking — lastCheckpointStatements uses getAndSet for atomic delta computation
    // lastCheckpointTime is volatile for visibility (slight race on concurrent checkpoints is
    // acceptable for metrics purposes — cumulative values are always correct).
    private final AtomicLong lastCheckpointStatements = new AtomicLong();
    private volatile Instant lastCheckpointTime;

    DefaultMappingExecution(
            @NonNull Flux<Statement> sourceFlux,
            @NonNull MappingExecutionObserver observer,
            @NonNull List<ResolvedMapping> resolvedMappings) {
        this.cancelSink = Sinks.empty();
        this.startedAt = Instant.now();
        this.lastCheckpointTime = startedAt;
        this.observer = observer;
        this.resolvedMappings = List.copyOf(resolvedMappings);

        this.statements = sourceFlux
                .takeUntilOther(cancelSink.asMono())
                .doOnNext(stmt -> statementsProduced.incrementAndGet())
                .doOnError(ex -> errorsEncountered.incrementAndGet());
    }

    @Override
    public Flux<Statement> statements() {
        return statements;
    }

    @Override
    public Mono<Void> cancel() {
        return Mono.fromRunnable(cancelSink::tryEmitEmpty);
    }

    @Override
    public Mono<Void> checkpoint() {
        return Mono.fromRunnable(() -> {
            var now = Instant.now();
            var totalStatements = statementsProduced.get();
            var prevStatements = lastCheckpointStatements.getAndSet(totalStatements);
            var prevTime = lastCheckpointTime;
            lastCheckpointTime = now;

            var checkpointInfo = new CheckpointInfo(
                    totalStatements - prevStatements,
                    totalStatements,
                    0, // iteration tracking deferred
                    0, // iteration tracking deferred
                    Duration.between(prevTime, now),
                    Duration.between(startedAt, now));

            for (var mapping : resolvedMappings) {
                observer.onCheckpoint(mapping, checkpointInfo);
            }
        });
    }

    @Override
    public MappingExecutionMetrics currentMetrics() {
        return new Snapshot(
                statementsProduced.get(),
                errorsEncountered.get(),
                startedAt,
                Duration.between(startedAt, Instant.now()));
    }

    private record Snapshot(long statementsProduced, long errorsEncountered, Instant startedAt, Duration elapsed)
            implements MappingExecutionMetrics {}
}
