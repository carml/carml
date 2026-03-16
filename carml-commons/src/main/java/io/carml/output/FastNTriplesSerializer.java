package io.carml.output;

import java.io.ByteArrayOutputStream;
import org.eclipse.rdf4j.model.Statement;

/**
 * A batch-oriented, byte-buffer based N-Triples serializer that bypasses Rio's per-statement
 * overhead.
 *
 * <p>Key optimizations over Rio's {@code NTriplesWriter}:
 * <ul>
 *   <li>Direct UTF-8 byte output -- no intermediate String, no OutputStreamWriter double-encoding</li>
 *   <li>Batch processing: serializes batches of statements into pre-allocated byte buffers</li>
 *   <li>Bounded LRU term cache for frequently repeated IRIs, blank nodes, and typed literals</li>
 *   <li>Batch serialization via reactive operators</li>
 * </ul>
 *
 * <p>Implements the <a href="https://www.w3.org/TR/n-triples/">W3C N-Triples</a> specification.
 *
 * <p><strong>Thread safety:</strong> Instances are not thread-safe. A single {@link #serialize}
 * call is safe (batches execute sequentially via {@code concatMap}), but concurrent
 * {@link #serialize} calls on the same instance will corrupt the LRU cache.
 */
public final class FastNTriplesSerializer extends AbstractFastRdfSerializer {

    private FastNTriplesSerializer(int batchSize, int cacheMaxSize) {
        super(batchSize, cacheMaxSize);
    }

    /**
     * Creates a new builder for configuring a {@code FastNTriplesSerializer}.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a {@code FastNTriplesSerializer} with default settings (batch size 4096, cache size
     * 65536).
     *
     * @return a new serializer instance with default settings
     */
    public static FastNTriplesSerializer withDefaults() {
        return new Builder().build();
    }

    @Override
    void writeStatement(Statement statement, ByteArrayOutputStream buffer) {
        writeResource(statement.getSubject(), buffer);
        writeBytes(SPACE, buffer);
        writeValue(statement.getPredicate(), buffer);
        writeBytes(SPACE, buffer);
        writeValue(statement.getObject(), buffer);
        writeBytes(DOT_NEWLINE, buffer);
    }

    /**
     * Builder for configuring {@code FastNTriplesSerializer} instances.
     */
    public static final class Builder extends AbstractBuilder<FastNTriplesSerializer, Builder> {

        private Builder() {}

        /**
         * Builds and returns a new {@code FastNTriplesSerializer} with the configured settings.
         *
         * @return a new serializer instance
         */
        @Override
        public FastNTriplesSerializer build() {
            return new FastNTriplesSerializer(batchSize, cacheMaxSize);
        }
    }
}
