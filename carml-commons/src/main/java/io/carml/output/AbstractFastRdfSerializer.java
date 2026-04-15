package io.carml.output;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import reactor.core.publisher.Flux;

/**
 * Abstract base class for high-throughput RDF serializers (N-Triples, N-Quads).
 *
 * <p>Contains all shared serialization logic: direct stream output, term encoding, literal
 * escaping, LRU caching, and the builder pattern. Subclasses only need to implement
 * {@link #writeStatement(Statement, OutputStream)} to define the per-statement output format.
 *
 * <p>Implements the {@link RdfSerializer} lifecycle ({@link #start}, {@link #write}, {@link #end},
 * {@link #close}) as well as the byte-level fast path ({@link #supportsByteEncoding()},
 * {@link #encode(Statement)}).
 *
 * <p>Key optimizations:
 * <ul>
 *   <li>Direct UTF-8 byte output -- no intermediate String, no OutputStreamWriter double-encoding</li>
 *   <li>Streaming output: writes each statement directly to a {@link BufferedOutputStream},
 *       eliminating per-batch byte array allocation and copying</li>
 *   <li>Bounded LRU term cache for frequently repeated IRIs and blank nodes</li>
 *   <li>Streaming codepoint iteration for literal escaping -- no intermediate int[] allocation</li>
 * </ul>
 *
 * <p><strong>Thread safety:</strong> Instances are not thread-safe. A single {@link #serialize}
 * call is safe (statements are written sequentially), but concurrent {@link #serialize} calls on
 * the same instance will corrupt the shared LRU cache.
 */
abstract class AbstractFastRdfSerializer implements RdfSerializer {

    static final byte[] SPACE = RdfTermEncoding.SPACE;

    static final byte[] DOT_NEWLINE = RdfTermEncoding.DOT_NEWLINE;

    static final int DEFAULT_CACHE_MAX_SIZE = 65536;

    /** Buffer size for {@link BufferedOutputStream} wrapping the caller's output stream. */
    private static final int OUTPUT_BUFFER_SIZE = 65536;

    private final LruTermCache termCache;

    /** The buffered output stream used by the {@link RdfSerializer} lifecycle methods. */
    private BufferedOutputStream lifecycleOutput;

    AbstractFastRdfSerializer(int cacheMaxSize) {
        this.termCache = new LruTermCache(cacheMaxSize);
    }

    // ---- RdfSerializer lifecycle ----

    @Override
    public void start(OutputStream output, Map<String, String> namespaces) {
        if (lifecycleOutput != null) {
            throw new IllegalStateException("start() called while a session is already active");
        }
        // N-Triples and N-Quads have no preamble; namespaces are ignored.
        this.lifecycleOutput = new BufferedOutputStream(output, OUTPUT_BUFFER_SIZE);
    }

    @Override
    public void write(Statement statement) {
        if (lifecycleOutput == null) {
            throw new IllegalStateException("write() called outside of an active serialization session");
        }
        writeStatement(statement, lifecycleOutput);
    }

    @Override
    public void flush() {
        if (lifecycleOutput != null) {
            try {
                lifecycleOutput.flush();
            } catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            }
        }
    }

    @Override
    public void end() {
        // N-Triples and N-Quads have no epilogue; flush and release the session so further write()
        // calls fail fast.
        flush();
        lifecycleOutput = null;
    }

    @Override
    public void close() {
        // Release the reference without closing the underlying output stream.
        lifecycleOutput = null;
    }

    // ---- Byte-level fast path ----

    @Override
    public boolean supportsByteEncoding() {
        return true;
    }

    @Override
    public byte[] encode(Statement statement) {
        return serializeStatement(statement);
    }

    // ---- Legacy Flux-based API (used by OutputHandler) ----

    /**
     * Serializes a {@link Flux} of {@link Statement}s to the provided {@link OutputStream}.
     * Statements are written directly through a {@link BufferedOutputStream} for throughput.
     *
     * <p>This is a standalone convenience method that does <strong>not</strong> use the
     * {@link RdfSerializer} lifecycle ({@link #start}/{@link #write}/{@link #end}). It creates
     * its own internal buffer and flushes on completion.
     *
     * @param statements the reactive stream of RDF statements to serialize
     * @param output the output stream to write serialized bytes to
     * @return the total number of statements serialized
     */
    public long serialize(Flux<Statement> statements, OutputStream output) {
        try {
            var buffered = new BufferedOutputStream(output, OUTPUT_BUFFER_SIZE);
            var count = statements
                    .doOnNext(statement -> writeStatement(statement, buffered))
                    .count()
                    .block();
            buffered.flush();
            return count != null ? count : 0;
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }

    /**
     * Serializes a single {@link Statement} to its byte representation.
     *
     * @param statement the RDF statement to serialize
     * @return the encoded bytes for the statement (including trailing newline)
     */
    public byte[] serializeStatement(Statement statement) {
        var buffer = new ByteArrayOutputStream(256);
        writeStatement(statement, buffer);
        return buffer.toByteArray();
    }

    /**
     * Writes a single statement to the output stream. Subclasses implement this to define the output
     * format (e.g., N-Triples writes subject-predicate-object; N-Quads adds an optional graph
     * field).
     *
     * @param statement the RDF statement to write
     * @param output the output stream to write to
     */
    abstract void writeStatement(Statement statement, OutputStream output);

    /**
     * Writes a subject resource (IRI or blank node) to the output stream.
     */
    void writeResource(Resource resource, OutputStream output) {
        RdfTermEncoding.writeResource(resource, output, termCache);
    }

    /**
     * Writes an object value (IRI, blank node, or literal) to the output stream.
     */
    void writeValue(Value value, OutputStream output) {
        RdfTermEncoding.writeValue(value, output, termCache);
    }

    static void writeBytes(byte[] bytes, OutputStream output) {
        RdfTermEncoding.writeBytes(bytes, output);
    }

    /**
     * Abstract builder base for configuring serializer instances.
     *
     * @param <S> the concrete serializer type
     * @param <B> the concrete builder type (for fluent returns)
     */
    abstract static class AbstractBuilder<S extends AbstractFastRdfSerializer, B extends AbstractBuilder<S, B>> {

        private int cacheMaxSize = DEFAULT_CACHE_MAX_SIZE;

        AbstractBuilder() {}

        protected int getCacheMaxSize() {
            return cacheMaxSize;
        }

        /**
         * Sets the maximum number of entries in the LRU term cache. When the cache exceeds this
         * size, the least recently accessed entry is evicted.
         *
         * @param cacheMaxSize the maximum cache size (must be positive)
         * @return this builder
         */
        public B cacheMaxSize(int cacheMaxSize) {
            if (cacheMaxSize <= 0) {
                throw new IllegalArgumentException("Cache max size must be positive, got %d".formatted(cacheMaxSize));
            }
            this.cacheMaxSize = cacheMaxSize;
            return self();
        }

        /**
         * Builds and returns a new serializer instance with the configured settings.
         *
         * @return a new serializer instance
         */
        public abstract S build();

        @SuppressWarnings("unchecked")
        private B self() {
            return (B) this;
        }
    }
}
