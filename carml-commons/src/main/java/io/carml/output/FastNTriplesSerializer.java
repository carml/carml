package io.carml.output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import reactor.core.publisher.Flux;

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
@Slf4j
public final class FastNTriplesSerializer {

    private static final byte[] SPACE = " ".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] DOT_NEWLINE = " .\n".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] QUOTE = "\"".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] LANG_TAG_PREFIX = "@".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] DATATYPE_PREFIX = "^^".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ESCAPE_TAB = "\\t".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ESCAPE_BACKSPACE = "\\b".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ESCAPE_NEWLINE = "\\n".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ESCAPE_FORMFEED = "\\f".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ESCAPE_CR = "\\r".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ESCAPE_QUOTE = "\\\"".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] ESCAPE_BACKSLASH = "\\\\".getBytes(StandardCharsets.US_ASCII);

    private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    private static final int DEFAULT_BATCH_SIZE = 4096;

    private static final int DEFAULT_CACHE_MAX_SIZE = 65536;

    private final int batchSize;

    private final LruTermCache termCache;

    private FastNTriplesSerializer(int batchSize, int cacheMaxSize) {
        this.batchSize = batchSize;
        this.termCache = new LruTermCache(cacheMaxSize);
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

    /**
     * Serializes a {@link Flux} of {@link Statement}s to the provided {@link OutputStream} in
     * N-Triples format. Statements are processed in batches for throughput, and each batch is
     * serialized in batches for throughput.
     *
     * @param statements the reactive stream of RDF statements to serialize
     * @param output the output stream to write N-Triples bytes to
     * @return the total number of statements serialized
     */
    public long serialize(Flux<Statement> statements, OutputStream output) {
        var allStatements = statements.collectList().block();
        if (allStatements == null || allStatements.isEmpty()) {
            return 0;
        }

        long count = 0;
        try {
            for (int i = 0; i < allStatements.size(); i += batchSize) {
                var end = Math.min(i + batchSize, allStatements.size());
                var batch = allStatements.subList(i, end);
                var bytes = serializeBatch(batch);
                output.write(bytes);
                count += batch.size();
            }
            output.flush();
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }

        return count;
    }

    /**
     * Serializes a single {@link Statement} to its N-Triples byte representation.
     *
     * @param statement the RDF statement to serialize
     * @return the N-Triples encoded bytes for the statement (including trailing newline)
     */
    public byte[] serializeStatement(Statement statement) {
        var buffer = new ByteArrayOutputStream(256);
        writeStatement(statement, buffer);
        return buffer.toByteArray();
    }

    /**
     * Returns the cached byte representation for a term, computing and caching it on first
     * encounter.
     *
     * @param term the RDF value to encode
     * @param encoder the function to compute the byte encoding if not cached
     * @return the cached or freshly computed byte encoding
     */
    byte[] getOrComputeCached(Value term, Function<Value, byte[]> encoder) {
        return termCache.getOrCompute(term, encoder);
    }

    private byte[] serializeBatch(List<Statement> batch) {
        var buffer = new ByteArrayOutputStream(batch.size() * 128);
        for (var statement : batch) {
            writeStatement(statement, buffer);
        }
        return buffer.toByteArray();
    }

    private void writeStatement(Statement statement, ByteArrayOutputStream buffer) {
        writeResource(statement.getSubject(), buffer);
        writeBytes(SPACE, buffer);
        writeIri(statement.getPredicate(), buffer);
        writeBytes(SPACE, buffer);
        writeValue(statement.getObject(), buffer);
        writeBytes(DOT_NEWLINE, buffer);
    }

    /**
     * Writes a subject resource (IRI or blank node) to the buffer.
     */
    void writeResource(Resource resource, ByteArrayOutputStream buffer) {
        if (resource instanceof IRI iri) {
            writeIri(iri, buffer);
        } else if (resource instanceof BNode bNode) {
            writeBNode(bNode, buffer);
        }
    }

    /**
     * Writes an object value (IRI, blank node, or literal) to the buffer.
     */
    void writeValue(Value value, ByteArrayOutputStream buffer) {
        if (value instanceof IRI iri) {
            writeIri(iri, buffer);
        } else if (value instanceof BNode bNode) {
            writeBNode(bNode, buffer);
        } else if (value instanceof Literal literal) {
            writeLiteral(literal, buffer);
        }
    }

    private void writeIri(IRI iri, ByteArrayOutputStream buffer) {
        var cached = termCache.getOrCompute(iri, FastNTriplesSerializer::encodeIri);
        writeBytes(cached, buffer);
    }

    private void writeBNode(BNode bNode, ByteArrayOutputStream buffer) {
        var cached = termCache.getOrCompute(bNode, FastNTriplesSerializer::encodeBNode);
        writeBytes(cached, buffer);
    }

    private void writeLiteral(Literal literal, ByteArrayOutputStream buffer) {
        writeBytes(QUOTE, buffer);
        writeEscapedLiteralBytes(literal.getLabel(), buffer);
        writeBytes(QUOTE, buffer);

        literal.getLanguage()
                .ifPresentOrElse(
                        language -> {
                            writeBytes(LANG_TAG_PREFIX, buffer);
                            writeBytes(language.getBytes(StandardCharsets.UTF_8), buffer);
                        },
                        () -> {
                            var datatype = literal.getDatatype();
                            if (datatype != null && !isXsdString(datatype)) {
                                writeBytes(DATATYPE_PREFIX, buffer);
                                writeIri(datatype, buffer);
                            }
                        });
    }

    private static boolean isXsdString(IRI datatype) {
        return "http://www.w3.org/2001/XMLSchema#string".equals(datatype.stringValue());
    }

    /**
     * Encodes an IRI value to its N-Triples byte representation ({@code <iri>}).
     *
     * <p>IRIs are assumed to be properly encoded (no prohibited characters such as spaces or angle
     * brackets). The method writes IRI bytes verbatim between angle brackets without additional
     * escaping.
     *
     * @param value the IRI value to encode
     * @return the N-Triples encoded bytes for the IRI
     */
    static byte[] encodeIri(Value value) {
        var iriString = value.stringValue();
        var iriBytes = iriString.getBytes(StandardCharsets.UTF_8);
        var result = new byte[iriBytes.length + 2];
        result[0] = '<';
        System.arraycopy(iriBytes, 0, result, 1, iriBytes.length);
        result[result.length - 1] = '>';
        return result;
    }

    static byte[] encodeBNode(Value value) {
        var idBytes = value.stringValue().getBytes(StandardCharsets.UTF_8);
        var result = new byte[idBytes.length + 2];
        result[0] = '_';
        result[1] = ':';
        System.arraycopy(idBytes, 0, result, 2, idBytes.length);
        return result;
    }

    /**
     * Writes escaped literal string bytes directly to the buffer following the N-Triples escaping
     * rules (W3C N-Triples spec, section 4):
     * <ul>
     *   <li>{@code \b} for U+0008 (backspace)</li>
     *   <li>{@code \t} for U+0009 (tab)</li>
     *   <li>{@code \n} for U+000A (newline)</li>
     *   <li>{@code \f} for U+000C (form feed)</li>
     *   <li>{@code \r} for U+000D (carriage return)</li>
     *   <li>{@code \"} for U+0022 (double quote)</li>
     *   <li>{@code \\} for U+005C (backslash)</li>
     *   <li>{@code \}uXXXX for U+0000-U+001F (except named escapes above) and U+007F-U+FFFF</li>
     *   <li>{@code \}UXXXXXXXX for codepoints above U+FFFF</li>
     * </ul>
     */
    static void writeEscapedLiteralBytes(String label, ByteArrayOutputStream buffer) {
        var codePoints = label.codePoints().toArray();

        for (var codePoint : codePoints) {
            switch (codePoint) {
                case '\b' -> writeBytes(ESCAPE_BACKSPACE, buffer);
                case '\t' -> writeBytes(ESCAPE_TAB, buffer);
                case '\n' -> writeBytes(ESCAPE_NEWLINE, buffer);
                case '\f' -> writeBytes(ESCAPE_FORMFEED, buffer);
                case '\r' -> writeBytes(ESCAPE_CR, buffer);
                case '"' -> writeBytes(ESCAPE_QUOTE, buffer);
                case '\\' -> writeBytes(ESCAPE_BACKSLASH, buffer);
                default -> {
                    if (codePoint >= 0x0000 && codePoint <= 0x001F) {
                        // Control characters (except \b, \t, \n, \f, \r handled above)
                        writeUnicodeEscape4(codePoint, buffer);
                    } else if (codePoint >= 0x007F && codePoint <= 0xFFFF) {
                        // Non-ASCII up to BMP
                        writeUnicodeEscape4(codePoint, buffer);
                    } else if (codePoint > 0xFFFF) {
                        // Supplementary plane characters
                        writeUnicodeEscape8(codePoint, buffer);
                    } else {
                        // Printable ASCII (0x0020 - 0x007E, excluding " and \ handled above)
                        buffer.write(codePoint);
                    }
                }
            }
        }
    }

    private static void writeUnicodeEscape4(int codePoint, ByteArrayOutputStream buffer) {
        // Writes backslash-uXXXX directly using hex digit lookup -- avoids String.formatted() overhead
        buffer.write('\\');
        buffer.write('u');
        buffer.write(HEX_DIGITS[(codePoint >> 12) & 0xF]);
        buffer.write(HEX_DIGITS[(codePoint >> 8) & 0xF]);
        buffer.write(HEX_DIGITS[(codePoint >> 4) & 0xF]);
        buffer.write(HEX_DIGITS[codePoint & 0xF]);
    }

    private static void writeUnicodeEscape8(int codePoint, ByteArrayOutputStream buffer) {
        // Writes backslash-UXXXXXXXX directly using hex digit lookup -- avoids String.formatted() overhead
        buffer.write('\\');
        buffer.write('U');
        buffer.write(HEX_DIGITS[(codePoint >> 28) & 0xF]);
        buffer.write(HEX_DIGITS[(codePoint >> 24) & 0xF]);
        buffer.write(HEX_DIGITS[(codePoint >> 20) & 0xF]);
        buffer.write(HEX_DIGITS[(codePoint >> 16) & 0xF]);
        buffer.write(HEX_DIGITS[(codePoint >> 12) & 0xF]);
        buffer.write(HEX_DIGITS[(codePoint >> 8) & 0xF]);
        buffer.write(HEX_DIGITS[(codePoint >> 4) & 0xF]);
        buffer.write(HEX_DIGITS[codePoint & 0xF]);
    }

    private static void writeBytes(byte[] bytes, ByteArrayOutputStream buffer) {
        buffer.write(bytes, 0, bytes.length);
    }

    /**
     * Access-ordered LRU cache mapping {@link Value} instances to their pre-encoded UTF-8 byte
     * representations. Uses {@link LinkedHashMap} with access-ordering and
     * {@link LinkedHashMap#removeEldestEntry} override for bounded eviction.
     */
    @SuppressWarnings("java:S2160") // equals/hashCode not needed — internal cache, never compared
    static final class LruTermCache extends LinkedHashMap<Value, byte[]> {

        @java.io.Serial
        private static final long serialVersionUID = 1L;

        private final int maxSize;

        LruTermCache(int maxSize) {
            super(Math.min(maxSize, 16), 0.75f, true);
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Value, byte[]> eldest) {
            return size() > maxSize;
        }

        /**
         * Returns the cached byte encoding for the given term, computing and caching it via the
         * supplied encoder function on first encounter.
         *
         * @param term the RDF value to look up or encode
         * @param encoder the function to compute the byte encoding if not yet cached
         * @return the byte encoding for the term
         */
        byte[] getOrCompute(Value term, java.util.function.Function<Value, byte[]> encoder) {
            return computeIfAbsent(term, encoder);
        }
    }

    /**
     * Builder for configuring {@code FastNTriplesSerializer} instances.
     */
    public static final class Builder {

        private int batchSize = DEFAULT_BATCH_SIZE;

        private int cacheMaxSize = DEFAULT_CACHE_MAX_SIZE;

        private Builder() {}

        /**
         * Sets the number of statements to buffer before serializing as a batch.
         *
         * @param batchSize the batch size (must be positive)
         * @return this builder
         */
        public Builder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be positive, got %d".formatted(batchSize));
            }
            this.batchSize = batchSize;
            return this;
        }

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
         * Builds and returns a new {@code FastNTriplesSerializer} with the configured settings.
         *
         * @return a new serializer instance
         */
        public FastNTriplesSerializer build() {
            return new FastNTriplesSerializer(batchSize, cacheMaxSize);
        }
    }
}
