package io.carml.output;

import java.io.OutputStream;
import org.eclipse.rdf4j.model.Statement;

/**
 * A high-throughput, byte-level N-Triples serializer that bypasses Rio's per-statement overhead.
 *
 * <p>Key optimizations over Rio's {@code NTriplesWriter}:
 * <ul>
 *   <li>Direct UTF-8 byte output -- no intermediate String, no OutputStreamWriter double-encoding</li>
 *   <li>Streaming output via {@link java.io.BufferedOutputStream} -- no per-batch byte array
 *       allocation or copying</li>
 *   <li>Bounded LRU term cache for frequently repeated IRIs and blank nodes</li>
 *   <li>Streaming codepoint iteration for literal escaping -- no intermediate int[] allocation</li>
 * </ul>
 *
 * <p>Implements the <a href="https://www.w3.org/TR/n-triples/">W3C N-Triples</a> specification.
 *
 * <p><strong>Thread safety:</strong> Instances are not thread-safe. A single {@link #serialize}
 * call is safe (statements are written sequentially), but concurrent {@link #serialize} calls on
 * the same instance will corrupt the LRU cache.
 */
public final class FastNTriplesSerializer extends AbstractFastRdfSerializer {

    private FastNTriplesSerializer(int cacheMaxSize) {
        super(cacheMaxSize);
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
    void writeStatement(Statement statement, OutputStream output) {
        writeResource(statement.getSubject(), output);
        writeBytes(SPACE, output);
        writeValue(statement.getPredicate(), output);
        writeBytes(SPACE, output);
        writeValue(statement.getObject(), output);
        writeBytes(DOT_NEWLINE, output);
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
            return new FastNTriplesSerializer(getCacheMaxSize());
        }
    }
}
