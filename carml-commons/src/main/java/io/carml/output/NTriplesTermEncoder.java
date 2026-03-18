package io.carml.output;

import java.io.ByteArrayOutputStream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

/**
 * Stateful encoder that encodes RDF terms and full N-Triples/N-Quads lines to {@code byte[]}
 * arrays. Uses an LRU term cache for repeated IRIs and blank nodes.
 *
 * <p>This encoder bypasses {@link org.eclipse.rdf4j.model.Statement} creation entirely, encoding
 * directly from RDF4J {@link Value} instances to UTF-8 bytes. Each encode method returns a complete
 * line including the {@code " .\n"} terminator.
 *
 * <p><strong>Thread safety:</strong> Instances are not thread-safe due to the shared LRU cache.
 * Use one instance per thread or per mapping pipeline.
 */
public final class NTriplesTermEncoder {

    private static final int DEFAULT_CACHE_MAX_SIZE = 65536;

    /** Estimated average line size for initial ByteArrayOutputStream capacity. */
    private static final int ESTIMATED_LINE_SIZE = 256;

    private final LruTermCache termCache;

    private NTriplesTermEncoder(int cacheMaxSize) {
        this.termCache = new LruTermCache(cacheMaxSize);
    }

    /**
     * Creates a new builder for configuring an {@code NTriplesTermEncoder}.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an {@code NTriplesTermEncoder} with default settings (cache size 65536).
     *
     * @return a new encoder instance with default settings
     */
    public static NTriplesTermEncoder withDefaults() {
        return new Builder().build();
    }

    /**
     * Encodes an N-Triples line: {@code subject predicate object .\n}.
     *
     * @param subject the subject resource (IRI or blank node)
     * @param predicate the predicate IRI
     * @param object the object value (IRI, blank node, or literal)
     * @return a complete N-Triples line as a byte array
     */
    public byte[] encodeNTriple(Resource subject, IRI predicate, Value object) {
        return encodeNQuad(subject, predicate, object, null);
    }

    /**
     * Encodes an N-Quads line: {@code subject predicate object graph .\n}. If graph is null, the
     * output is an N-Triples line (no graph field).
     *
     * @param subject the subject resource (IRI or blank node)
     * @param predicate the predicate IRI
     * @param object the object value (IRI, blank node, or literal)
     * @param graph the optional graph resource (IRI or blank node); null for default graph
     * @return a complete N-Quads (or N-Triples) line as a byte array
     */
    public byte[] encodeNQuad(Resource subject, IRI predicate, Value object, Resource graph) {
        var buffer = new ByteArrayOutputStream(ESTIMATED_LINE_SIZE);
        RdfTermEncoding.writeResource(subject, buffer, termCache);
        RdfTermEncoding.writeBytes(RdfTermEncoding.SPACE, buffer);
        RdfTermEncoding.writeValue(predicate, buffer, termCache);
        RdfTermEncoding.writeBytes(RdfTermEncoding.SPACE, buffer);
        RdfTermEncoding.writeValue(object, buffer, termCache);
        if (graph != null) {
            RdfTermEncoding.writeBytes(RdfTermEncoding.SPACE, buffer);
            RdfTermEncoding.writeResource(graph, buffer, termCache);
        }
        RdfTermEncoding.writeBytes(RdfTermEncoding.DOT_NEWLINE, buffer);
        return buffer.toByteArray();
    }

    /**
     * Builder for configuring {@code NTriplesTermEncoder} instances.
     */
    public static final class Builder {

        private int cacheMaxSize = DEFAULT_CACHE_MAX_SIZE;

        private Builder() {}

        /**
         * Sets the maximum number of entries in the LRU term cache. When the cache exceeds this
         * size, the least recently accessed entry is evicted.
         *
         * @param cacheMaxSize the maximum cache size (must be positive)
         * @return this builder
         */
        public Builder cacheMaxSize(int cacheMaxSize) {
            if (cacheMaxSize <= 0) {
                throw new IllegalArgumentException("Cache max size must be positive, got %d".formatted(cacheMaxSize));
            }
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        /**
         * Builds and returns a new {@code NTriplesTermEncoder} with the configured settings.
         *
         * @return a new encoder instance
         */
        public NTriplesTermEncoder build() {
            return new NTriplesTermEncoder(cacheMaxSize);
        }
    }
}
