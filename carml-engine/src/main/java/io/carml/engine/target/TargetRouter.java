package io.carml.engine.target;

import io.carml.engine.CheckpointInfo;
import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.MappingExecutionResult;
import io.carml.engine.ResolvedMapping;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import io.carml.model.TermMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Statement;

/**
 * Routes generated RDF statements to their declared {@link LogicalTarget}s. Implements
 * {@link MappingExecutionObserver} to intercept statement generation events and dispatch them to the
 * appropriate {@link TargetWriter} instances.
 *
 * <p>Each statement is routed based on the {@link LogicalTarget}s declared on the {@link TermMap}
 * that produced it. Statements without explicit logical targets are written to the default writer
 * (if configured), representing the default output (e.g. stdout or an output file).
 *
 * <p>Lifecycle:
 * <ul>
 *   <li>{@link #open()} opens all registered target writers — call before mapping starts. Safe to
 *       invoke concurrently; only the first caller actually opens the writers. If opening a writer
 *       fails partway through, any writers that were already opened are closed before the exception
 *       is rethrown and the router is marked closed (terminal state) so the caller can release
 *       resources via {@link #close()}.
 *   <li>{@link #onMappingStart(ResolvedMapping)} lazily invokes {@link #open()} on the first
 *       mapping-start event if the caller did not already open the router. It performs no other
 *       lifecycle work.
 *   <li>{@link #onStatementGenerated} routes each statement to its target writer(s). If a declared
 *       logical target has no registered writer, the routing call throws
 *       {@link IllegalStateException} — a missing writer at runtime is a configuration bug that
 *       should surface as an error rather than a silent dropped statement.
 *   <li>{@link #onCheckpoint} flushes all writers.
 *   <li>{@link #onMappingComplete} is a no-op at the router level. The router does NOT close
 *       writers on mapping completion because {@code onMappingComplete} can fire without a matching
 *       {@code onMappingStart} (empty source fluxes, filtered views, and upstream errors cause
 *       {@link io.carml.engine.RmlMapper} to fire {@code onMappingComplete} on both
 *       {@code doOnComplete} and {@code doOnError} even though {@code onMappingStart} fires only
 *       when the first iteration reaches the mapper). A counter-driven auto-close would decrement
 *       without a matching increment in these cases and close writers while other concurrent
 *       mappings are still writing. The router's close is instead driven exclusively by the
 *       caller.
 *   <li>{@link #close()} closes all writers. Safe to invoke multiple times; additional calls after
 *       the first are no-ops. {@link TargetWriter#close()} is idempotent per its contract, so
 *       overlapping close paths are safe.
 * </ul>
 *
 * <p><strong>Caller-driven lifecycle:</strong> the router is {@link AutoCloseable}. Callers MUST
 * wrap the router in try-with-resources or call {@link #close()} explicitly. Mapping lifecycle
 * events ({@link #onMappingStart}, {@link #onMappingComplete}) do NOT close writers — they only
 * track mapping-start for lazy open. This is a deliberate design choice: it makes the router
 * robust against the {@code onMappingComplete}-without-{@code onMappingStart} firing patterns
 * described above.
 *
 * <p><strong>Single-use lifecycle:</strong> a {@code TargetRouter} instance is single-use. After
 * {@link #close()} has been invoked — explicitly or via try-with-resources — any subsequent
 * {@link #open()} call throws {@link IllegalStateException}. Similarly, if {@link #open()} fails
 * partway (see above), the router is left terminally closed. To run another mapping execution,
 * construct a fresh {@code TargetRouter} with fresh {@link TargetWriter} instances.
 *
 * <p>Thread safety: lifecycle transitions ({@code open}/{@code close}) are guarded by an internal
 * lock. No other synchronization is needed because the counter-driven auto-close has been removed
 * in favor of explicit caller-driven close. {@link #onStatementGenerated}, {@link #onCheckpoint}
 * and other hot-path operations delegate to the underlying {@link TargetWriter} instances without
 * additional synchronization. The writer implementations are responsible for thread-safe
 * {@link TargetWriter#write(Statement)}, {@link TargetWriter#flush()} and
 * {@link TargetWriter#close()} semantics when invoked from multiple reactive threads.
 */
@Slf4j
public class TargetRouter implements MappingExecutionObserver, AutoCloseable {

    private final Map<LogicalTarget, TargetWriter> writers;

    private final TargetWriter defaultWriter;

    private final Object lifecycleLock = new Object();

    private boolean opened;

    /**
     * Terminal close flag. Declared {@code volatile} so unsynchronized hot-path reads (e.g.
     * {@link #onCheckpoint} early-return, {@link #onStatementGenerated} readers) observe the
     * transition published by the synchronized {@link #close()}.
     */
    private volatile boolean closed;

    /**
     * Creates a TargetRouter with the given writers and optional default writer.
     *
     * @param writers mapping from LogicalTarget to its TargetWriter
     * @param defaultWriter writer for statements without explicit targets, or {@code null} to
     *     silently drop them
     */
    public TargetRouter(Map<LogicalTarget, TargetWriter> writers, TargetWriter defaultWriter) {
        this.writers = Map.copyOf(writers);
        this.defaultWriter = defaultWriter;
    }

    /**
     * Opens all registered target writers. Called automatically on the first
     * {@link #onMappingStart(ResolvedMapping)} invocation, or can be called explicitly before
     * mapping execution starts.
     *
     * <p>Concurrent callers serialize on an internal lock; only the first caller actually opens
     * the writers. Subsequent calls while already open are no-ops.
     *
     * <p>If a writer's {@link TargetWriter#open()} throws (e.g. disk full, permission denied),
     * any writers that were already opened in this call are closed in best-effort fashion, the
     * router is marked terminally closed, and the original exception is rethrown. The caller
     * should release resources via {@link #close()} (a subsequent {@code close()} call is a no-op
     * because the router is already in the closed state). Retrying requires constructing a new
     * {@code TargetRouter} with fresh {@link TargetWriter} instances because
     * {@link TargetWriter#open()} is not re-entrant after close.
     *
     * @throws IllegalStateException if the router has already been {@link #close() closed}
     */
    public void open() {
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("TargetRouter has been closed and cannot be reopened");
            }
            if (opened) {
                return;
            }
            openAllWriters();
            opened = true;
            LOG.debug("Opened {} target writers", writers.size() + (defaultWriter != null ? 1 : 0));
        }
    }

    @Override
    public void onMappingStart(ResolvedMapping mapping) {
        open();
    }

    @Override
    public void onStatementGenerated(
            ResolvedMapping mapping, ViewIteration source, Statement statement, TermMap termMap) {
        var logicalTargets = termMap.getLogicalTargets();

        if (logicalTargets.isEmpty()) {
            if (defaultWriter != null) {
                defaultWriter.write(statement);
            }
        } else {
            for (var logicalTarget : logicalTargets) {
                var writer = writers.get(logicalTarget);
                if (writer == null) {
                    throw new IllegalStateException(
                            "No writer registered for logical target %s".formatted(logicalTarget));
                }
                writer.write(statement);
            }
        }
    }

    @Override
    public void onCheckpoint(ResolvedMapping mapping, CheckpointInfo checkpoint) {
        if (closed) {
            return;
        }
        writers.values().forEach(TargetWriter::flush);
        if (defaultWriter != null) {
            defaultWriter.flush();
        }
    }

    /**
     * No-op at the router level. Close is driven by the caller's try-with-resources on the router,
     * not by mapping completion events. This is required because {@code onMappingComplete} can
     * fire without a matching {@code onMappingStart} (empty source fluxes, filtered views,
     * upstream errors), so a counter-driven auto-close would prematurely close writers while
     * other concurrent mappings are still writing.
     */
    @Override
    public void onMappingComplete(ResolvedMapping mapping, MappingExecutionResult result) {
        // Intentionally empty. See class-level and method-level Javadoc for rationale.
    }

    /**
     * Closes all registered writers and marks the router as closed. Safe to invoke multiple times;
     * additional calls after the first are no-ops. {@link TargetWriter#close()} implementations are
     * required to be idempotent (see {@link TargetWriter}), so writer closure is robust against
     * overlap with any other close path (e.g. rollback from a failed {@link #open()}).
     */
    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            closeAllWriters();
        }
    }

    /**
     * Returns the set of logical targets this router handles.
     */
    public Set<LogicalTarget> getLogicalTargets() {
        return writers.keySet();
    }

    /**
     * Returns whether this router has a default writer for unrouted statements.
     */
    public boolean hasDefaultWriter() {
        return defaultWriter != null;
    }

    /**
     * Opens all writers (registered + default). If any writer throws during open, already-opened
     * writers are closed best-effort, the router is marked terminally closed, and the original
     * exception is rethrown so the router is left in a consistent, non-open state. Callers hold
     * {@link #lifecycleLock} for the duration.
     */
    private void openAllWriters() {
        List<TargetWriter> openedSoFar = new ArrayList<>(writers.size() + 1);
        try {
            for (var writer : writers.values()) {
                writer.open();
                openedSoFar.add(writer);
            }
            if (defaultWriter != null) {
                defaultWriter.open();
                openedSoFar.add(defaultWriter);
            }
        } catch (RuntimeException openFailure) {
            rollbackPartialOpen(openedSoFar, openFailure);
            // Mark the router terminally closed so a subsequent caller close() (e.g. via
            // try-with-resources) is a no-op and does not invoke close() on writers that were
            // already closed by rollback (or on the writer whose open() threw, where close()
            // behavior is undefined per TargetWriter contract).
            closed = true;
            throw openFailure;
        }
    }

    private void rollbackPartialOpen(List<TargetWriter> openedSoFar, RuntimeException openFailure) {
        for (var writer : openedSoFar) {
            try {
                writer.close();
            } catch (RuntimeException closeFailure) {
                openFailure.addSuppressed(closeFailure);
                LOG.warn("Error closing writer during open() rollback", closeFailure);
            }
        }
    }

    private void closeAllWriters() {
        for (var writer : writers.values()) {
            try {
                writer.close();
            } catch (RuntimeException ex) {
                LOG.warn("Error closing target writer", ex);
            }
        }
        if (defaultWriter != null) {
            try {
                defaultWriter.close();
            } catch (RuntimeException ex) {
                LOG.warn("Error closing default target writer", ex);
            }
        }
        LOG.debug("Closed all target writers");
    }
}
