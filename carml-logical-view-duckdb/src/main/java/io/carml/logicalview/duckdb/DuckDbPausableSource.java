package io.carml.logicalview.duckdb;

import io.carml.logicalsourceresolver.PausableSource;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * A {@link PausableSource} for DuckDB's thread-bound JDBC/Arrow iteration.
 *
 * <p>Unlike JSON/XPath resolvers that break their read loop on pause and re-enter on resume,
 * DuckDB's {@code ResultSet} and Arrow reader are thread-bound — they cannot be accessed from a
 * different thread. This implementation parks the producer thread via {@link LockSupport#park(Object)}
 * when demand is exhausted, and unparks it when new demand arrives.
 *
 * <p>The emission loop calls {@link #awaitDemand()} after each {@code emitter.next()} call. If the
 * source has been paused (demand exhausted), the producer thread parks until resumed or cancelled.
 * The park loop handles spurious wakeups.
 */
class DuckDbPausableSource implements PausableSource {

    private final Consumer<DuckDbPausableSource> task;

    private volatile boolean paused;
    private volatile boolean completed;
    private volatile boolean cancelled;
    private final AtomicReference<Thread> producerThread = new AtomicReference<>();

    /**
     * Creates a new pausable source.
     *
     * @param task the emission task to run; receives {@code this} so the task can call
     *     {@link #awaitDemand()} and {@link #isCancelled()}
     */
    DuckDbPausableSource(Consumer<DuckDbPausableSource> task) {
        this.task = task;
    }

    @Override
    public void start() {
        producerThread.set(Thread.currentThread());
        task.accept(this);
    }

    @Override
    public void pause() {
        paused = true;
    }

    @Override
    public void resume() {
        paused = false;
        var thread = producerThread.get();
        if (thread != null) {
            LockSupport.unpark(thread);
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

    /**
     * Marks this source as completed. Called by the emission task after all rows have been emitted.
     */
    void complete() {
        completed = true;
    }

    /**
     * Returns {@code true} if the subscription has been cancelled. The emission loop should check
     * this to exit early.
     */
    boolean isCancelled() {
        return cancelled;
    }

    /**
     * Cancels the source. Called on subscription disposal to ensure the producer thread exits
     * cleanly.
     */
    void cancel() {
        cancelled = true;
        var thread = producerThread.get();
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }

    /**
     * Called by the emission loop after each {@code emitter.next()} call. If the source has been
     * paused (demand exhausted), parks the producer thread until resumed or cancelled. The loop
     * handles spurious wakeups from {@link LockSupport#park(Object)}.
     */
    void awaitDemand() {
        while (paused && !cancelled) {
            LockSupport.park(this);
        }
    }
}
