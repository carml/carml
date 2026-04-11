package io.carml.output;

import java.io.OutputStream;
import java.util.Map;
import org.eclipse.rdf4j.model.Statement;

/**
 * Serializes RDF statements to an output stream in a specific RDF format. Implements a
 * streaming-compatible lifecycle: {@link #start}, {@link #write}, {@link #end}.
 *
 * <p>Usage:
 * <pre>{@code
 * RdfSerializer serializer = provider.createSerializer("ttl", SerializerMode.STREAMING);
 * serializer.start(outputStream, namespaces);
 * for (Statement stmt : statements) {
 *     serializer.write(stmt);
 * }
 * serializer.end();
 * serializer.close();
 * }</pre>
 *
 * <p>For line-based formats (N-Triples, N-Quads), serializers may support a byte-level fast path
 * via {@link #supportsByteEncoding()} and {@link #encode(Statement)}, which allows encoding
 * individual statements to {@code byte[]} arrays without requiring a prior {@link #start} call.
 *
 * <p><strong>Thread safety:</strong> Implementations are not required to be thread-safe. Callers
 * must ensure external synchronization when sharing a serializer across threads.
 *
 * @see RdfSerializerProvider
 * @see SerializerMode
 */
public interface RdfSerializer extends AutoCloseable {

    /**
     * Starts serialization to the given output stream. Must be called before any
     * {@link #write(Statement)} calls. Writes any format-specific preamble (e.g. namespace
     * declarations for Turtle).
     *
     * <p>For {@link SerializerMode#BYTE_LEVEL} serializers, this method is a no-op — calling it
     * is optional since {@link #encode(Statement)} does not require prior initialization.
     *
     * @param output the output stream to write serialized bytes to
     * @param namespaces namespace prefix-to-IRI mappings to include in the output; may be empty
     *     but must not be null
     */
    void start(OutputStream output, Map<String, String> namespaces);

    /**
     * Writes a single RDF statement to the output stream.
     *
     * @param statement the RDF statement to serialize
     * @throws IllegalStateException if {@link #start} has not been called
     */
    void write(Statement statement);

    /**
     * Ends serialization, writing any format-specific epilogue and flushing the output. After
     * calling this method, no further {@link #write(Statement)} calls are allowed. The underlying
     * output stream is <strong>not</strong> closed — call {@link #close()} to release resources.
     *
     * <p>For {@link SerializerMode#BYTE_LEVEL} serializers, this method is a no-op.
     */
    void end();

    /**
     * Flushes any internal buffers and the underlying output stream. The exact behavior depends
     * on the {@link SerializerMode}:
     * <ul>
     *   <li>{@link SerializerMode#STREAMING} — flushes the writer's internal buffer and the
     *       underlying output stream.</li>
     *   <li>{@link SerializerMode#PRETTY} — may be a no-op, since output is buffered until
     *       {@link #end()} is called.</li>
     *   <li>{@link SerializerMode#BYTE_LEVEL} — no-op.</li>
     * </ul>
     *
     * <p>Called by {@code TargetWriter} on checkpoint events during long-running or streaming
     * executions.
     */
    default void flush() {
        // no-op by default
    }

    /**
     * Returns whether this serializer supports byte-level encoding of individual statements via
     * {@link #encode(Statement)}. Only line-based formats (N-Triples, N-Quads) typically support
     * this.
     *
     * @return {@code true} if {@link #encode(Statement)} is available
     */
    default boolean supportsByteEncoding() {
        return false;
    }

    /**
     * Encodes a single RDF statement to its byte representation. This is a stateless operation
     * that is always safe to call without a prior {@link #start} call, enabling high-throughput
     * pipelines where statements are encoded independently.
     *
     * <p>Only available when {@link #supportsByteEncoding()} returns {@code true}.
     *
     * @param statement the RDF statement to encode
     * @return the encoded bytes for the statement (including any line terminator)
     * @throws UnsupportedOperationException if byte-level encoding is not supported
     */
    default byte[] encode(Statement statement) {
        throw new UnsupportedOperationException("Byte-level encoding not supported by this serializer");
    }

    /**
     * Releases any resources held by this serializer. Does <strong>not</strong> close the
     * underlying output stream passed to {@link #start}. Safe to call multiple times.
     */
    @Override
    void close();
}
