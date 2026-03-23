package io.carml.output;

import java.io.OutputStream;
import org.eclipse.rdf4j.model.Statement;

/**
 * A high-throughput, byte-level N-Quads serializer that extends N-Triples with an optional graph
 * field.
 *
 * <p>N-Quads extends N-Triples with an optional 4th field (graph name) between the object and the
 * dot. When the graph context is absent, the output is identical to N-Triples (default graph).
 *
 * <p>Key optimizations (shared with {@link FastNTriplesSerializer}):
 * <ul>
 *   <li>Direct UTF-8 byte output -- no intermediate String, no OutputStreamWriter double-encoding</li>
 *   <li>Streaming output via {@link java.io.BufferedOutputStream} -- no per-batch byte array
 *       allocation or copying</li>
 *   <li>Bounded LRU term cache for frequently repeated IRIs and blank nodes</li>
 *   <li>Streaming codepoint iteration for literal escaping -- no intermediate int[] allocation</li>
 * </ul>
 *
 * <p>Implements the <a href="https://www.w3.org/TR/n-quads/">W3C N-Quads</a> specification.
 *
 * <p><strong>Thread safety:</strong> Instances are not thread-safe. A single {@link #serialize}
 * call is safe (statements are written sequentially), but concurrent {@link #serialize} calls on
 * the same instance will corrupt the shared LRU cache.
 *
 * @see FastNTriplesSerializer
 */
public final class FastNQuadsSerializer extends AbstractFastRdfSerializer {

    private FastNQuadsSerializer(int cacheMaxSize) {
        super(cacheMaxSize);
    }

    /**
     * Creates a new builder for configuring a {@code FastNQuadsSerializer}.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a {@code FastNQuadsSerializer} with default settings (batch size 4096, cache size
     * 65536).
     *
     * @return a new serializer instance with default settings
     */
    public static FastNQuadsSerializer withDefaults() {
        return new Builder().build();
    }

    @Override
    void writeStatement(Statement statement, OutputStream output) {
        writeResource(statement.getSubject(), output);
        writeBytes(SPACE, output);
        writeValue(statement.getPredicate(), output);
        writeBytes(SPACE, output);
        writeValue(statement.getObject(), output);

        var context = statement.getContext();
        if (context != null) {
            writeBytes(SPACE, output);
            writeResource(context, output);
        }

        writeBytes(DOT_NEWLINE, output);
    }

    /**
     * Builder for configuring {@code FastNQuadsSerializer} instances.
     */
    public static final class Builder extends AbstractBuilder<FastNQuadsSerializer, Builder> {

        private Builder() {}

        /**
         * Builds and returns a new {@code FastNQuadsSerializer} with the configured settings.
         *
         * @return a new serializer instance
         */
        @Override
        public FastNQuadsSerializer build() {
            return new FastNQuadsSerializer(getCacheMaxSize());
        }
    }
}
