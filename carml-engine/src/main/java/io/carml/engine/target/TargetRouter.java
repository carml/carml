package io.carml.engine.target;

import io.carml.engine.CheckpointInfo;
import io.carml.engine.MappingExecutionObserver;
import io.carml.engine.MappingExecutionResult;
import io.carml.engine.ResolvedMapping;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalTarget;
import io.carml.model.TermMap;
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
 *   <li>{@link #open()} opens all registered target writers — call before mapping starts
 *   <li>{@link #onStatementGenerated} routes each statement to its target writer(s)
 *   <li>{@link #onCheckpoint} flushes all writers
 *   <li>{@link #onMappingComplete} closes all writers when the last mapping finishes
 *   <li>{@link #close()} force-closes all writers (cleanup safety net)
 * </ul>
 *
 * <p>Thread safety: write operations delegate to individual {@link TargetWriter} instances. If
 * multiple reactive threads invoke {@link #onStatementGenerated} concurrently, the caller must
 * ensure external synchronization or use thread-safe writer implementations.
 */
@Slf4j
public class TargetRouter implements MappingExecutionObserver, AutoCloseable {

    private final Map<LogicalTarget, TargetWriter> writers;

    private final TargetWriter defaultWriter;

    private int activeMappings;

    private boolean opened;

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
     */
    public void open() {
        if (opened) {
            return;
        }
        opened = true;
        writers.values().forEach(TargetWriter::open);
        if (defaultWriter != null) {
            defaultWriter.open();
        }
        LOG.debug("Opened {} target writers", writers.size() + (defaultWriter != null ? 1 : 0));
    }

    @Override
    public void onMappingStart(ResolvedMapping mapping) {
        open();
        activeMappings++;
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
                if (writer != null) {
                    writer.write(statement);
                } else {
                    LOG.warn("No writer registered for logical target {}", logicalTarget);
                }
            }
        }
    }

    @Override
    public void onCheckpoint(ResolvedMapping mapping, CheckpointInfo checkpoint) {
        writers.values().forEach(TargetWriter::flush);
        if (defaultWriter != null) {
            defaultWriter.flush();
        }
    }

    @Override
    public void onMappingComplete(ResolvedMapping mapping, MappingExecutionResult result) {
        activeMappings--;
        if (activeMappings <= 0) {
            closeAllWriters();
        }
    }

    @Override
    public void close() {
        closeAllWriters();
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
