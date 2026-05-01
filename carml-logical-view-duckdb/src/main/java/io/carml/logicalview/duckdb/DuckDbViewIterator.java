package io.carml.logicalview.duckdb;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;

/**
 * Pull-based iterator over {@link DuckDbViewIteration}s for use with
 * {@link reactor.core.publisher.Flux#fromIterable}. Holds at most one batch worth of materialized
 * iterations in memory at a time, providing natural backpressure: items are produced only when
 * {@link Iter#next()} is called, and {@link Iter#next()} is only called when the downstream subscriber has
 * outstanding demand.
 *
 * <p>Implements {@link Iterable} so it can be passed directly to
 * {@code Flux.fromIterable(iterable)}; the Iterable is single-shot — only one iterator can be
 * obtained per instance.
 *
 * <p>Resources owned by this iterator (the JDBC {@code Statement}, {@code ResultSet}, and any Arrow
 * reader) must be closed via {@link #close()}, which is wired into {@code Flux.using}'s resource
 * cleanup callback.
 */
@Slf4j
class DuckDbViewIterator implements Iterable<DuckDbViewIteration>, AutoCloseable {

    private final BatchLoader batchLoader;
    private final Deque<DuckDbViewIteration> buffer = new ArrayDeque<>();
    private boolean exhausted;
    private boolean iteratorReturned;
    private final Runnable cleanup;

    DuckDbViewIterator(BatchLoader batchLoader, Runnable cleanup) {
        this.batchLoader = batchLoader;
        this.cleanup = cleanup;
    }

    @Override
    public Iterator<DuckDbViewIteration> iterator() {
        if (iteratorReturned) {
            throw new IllegalStateException("DuckDbViewIterator is single-shot; iterator() called more than once");
        }
        iteratorReturned = true;
        return new Iter();
    }

    @Override
    public void close() {
        try {
            batchLoader.close();
        } catch (Exception e) {
            LOG.debug("Error closing DuckDB batch loader", e);
        } finally {
            cleanup.run();
        }
    }

    /**
     * Produces batches of {@link DuckDbViewIteration}s on demand. {@link #loadInto} fills the buffer
     * with the next batch and returns {@code true} if any items were added. Returns {@code false}
     * when the source is exhausted.
     */
    interface BatchLoader extends AutoCloseable {
        boolean loadInto(Deque<DuckDbViewIteration> buffer);

        @Override
        void close();
    }

    private final class Iter implements Iterator<DuckDbViewIteration> {

        @Override
        public boolean hasNext() {
            if (!buffer.isEmpty()) {
                return true;
            }
            if (exhausted) {
                return false;
            }
            // Loop until the loader either fills the buffer or signals exhaustion. Empty batches
            // (e.g. zero-row Arrow batches) are silently skipped.
            while (buffer.isEmpty()) {
                if (!batchLoader.loadInto(buffer)) {
                    exhausted = true;
                    return false;
                }
            }
            return true;
        }

        @Override
        public DuckDbViewIteration next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return buffer.poll();
        }
    }
}
