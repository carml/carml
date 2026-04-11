package io.carml.engine.target;

import org.eclipse.rdf4j.model.Statement;

/**
 * Writes RDF statements to a specific target with configurable serialization, compression, and
 * encoding. Implements a streaming-compatible lifecycle: {@link #open()}, {@link #write(Statement)},
 * {@link #flush()}, {@link #close()}.
 *
 * <p>Implementations must be safe to call {@link #write(Statement)} from multiple threads if used
 * from a {@code TargetRouter} observer context. The router ensures external synchronization.
 */
public interface TargetWriter extends AutoCloseable {

    /**
     * Opens the target for writing. Must be called before any {@link #write(Statement)} calls.
     * Creates underlying output resources (files, connections, etc.) and initializes the RDF
     * serialization writer.
     */
    void open();

    /**
     * Writes a single RDF statement to the target.
     *
     * @param statement the RDF statement to write
     */
    void write(Statement statement);

    /**
     * Flushes any buffered output to the underlying target. Called periodically during long-running
     * or streaming executions via checkpoint events.
     */
    void flush();

    /**
     * Closes the target, finalizing any RDF serialization output and releasing underlying resources.
     * After closing, no further {@link #write(Statement)} calls are allowed.
     */
    @Override
    void close();
}
