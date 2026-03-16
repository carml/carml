package io.carml.output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serial;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import reactor.core.publisher.Flux;

/**
 * Abstract base class for batch-oriented, byte-buffer based RDF serializers (N-Triples, N-Quads).
 *
 * <p>Contains all shared serialization logic: batch processing, term encoding, literal escaping,
 * LRU caching, and the builder pattern. Subclasses only need to implement
 * {@link #writeStatement(Statement, ByteArrayOutputStream)} to define the per-statement output
 * format.
 *
 * <p>Key optimizations:
 * <ul>
 *   <li>Direct UTF-8 byte output -- no intermediate String, no OutputStreamWriter double-encoding</li>
 *   <li>Batch processing: serializes batches of statements into pre-allocated byte buffers</li>
 *   <li>Bounded LRU term cache for frequently repeated IRIs, blank nodes, and typed literals</li>
 * </ul>
 *
 * <p><strong>Thread safety:</strong> Instances are not thread-safe. A single {@link #serialize}
 * call is safe (batches execute sequentially), but concurrent {@link #serialize} calls on the same
 * instance will corrupt the shared LRU cache.
 */
abstract class AbstractFastRdfSerializer {

    static final byte[] SPACE = " ".getBytes(StandardCharsets.US_ASCII);

    static final byte[] DOT_NEWLINE = " .\n".getBytes(StandardCharsets.US_ASCII);

    static final byte[] QUOTE = "\"".getBytes(StandardCharsets.US_ASCII);

    static final byte[] LANG_TAG_PREFIX = "@".getBytes(StandardCharsets.US_ASCII);

    static final byte[] DATATYPE_PREFIX = "^^".getBytes(StandardCharsets.US_ASCII);

    static final byte[] ESCAPE_TAB = "\\t".getBytes(StandardCharsets.US_ASCII);

    static final byte[] ESCAPE_BACKSPACE = "\\b".getBytes(StandardCharsets.US_ASCII);

    static final byte[] ESCAPE_NEWLINE = "\\n".getBytes(StandardCharsets.US_ASCII);

    static final byte[] ESCAPE_FORMFEED = "\\f".getBytes(StandardCharsets.US_ASCII);

    static final byte[] ESCAPE_CR = "\\r".getBytes(StandardCharsets.US_ASCII);

    static final byte[] ESCAPE_QUOTE = "\\\"".getBytes(StandardCharsets.US_ASCII);

    static final byte[] ESCAPE_BACKSLASH = "\\\\".getBytes(StandardCharsets.US_ASCII);

    static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    static final int DEFAULT_BATCH_SIZE = 4096;

    static final int DEFAULT_CACHE_MAX_SIZE = 65536;

    private final int batchSize;

    private final LruTermCache termCache;

    AbstractFastRdfSerializer(int batchSize, int cacheMaxSize) {
        this.batchSize = batchSize;
        this.termCache = new LruTermCache(cacheMaxSize);
    }

    /**
     * Serializes a {@link Flux} of {@link Statement}s to the provided {@link OutputStream}.
     * Statements are processed in batches for throughput.
     *
     * @param statements the reactive stream of RDF statements to serialize
     * @param output the output stream to write serialized bytes to
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
     * Writes a single statement to the buffer. Subclasses implement this to define the output format
     * (e.g., N-Triples writes subject-predicate-object; N-Quads adds an optional graph field).
     *
     * @param statement the RDF statement to write
     * @param buffer the byte buffer to write to
     */
    abstract void writeStatement(Statement statement, ByteArrayOutputStream buffer);

    private byte[] serializeBatch(List<Statement> batch) {
        var buffer = new ByteArrayOutputStream(batch.size() * 128);
        for (var statement : batch) {
            writeStatement(statement, buffer);
        }
        return buffer.toByteArray();
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
        var cached = termCache.getOrCompute(iri, AbstractFastRdfSerializer::encodeIri);
        writeBytes(cached, buffer);
    }

    private void writeBNode(BNode bNode, ByteArrayOutputStream buffer) {
        var cached = termCache.getOrCompute(bNode, AbstractFastRdfSerializer::encodeBNode);
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
        return XSD.STRING.equals(datatype);
    }

    /**
     * Encodes an IRI value to its N-Triples/N-Quads byte representation ({@code <iri>}).
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

    static void writeBytes(byte[] bytes, ByteArrayOutputStream buffer) {
        buffer.write(bytes, 0, bytes.length);
    }

    /**
     * Access-ordered LRU cache mapping {@link Value} instances to their pre-encoded UTF-8 byte
     * representations. Uses {@link LinkedHashMap} with access-ordering and
     * {@link LinkedHashMap#removeEldestEntry} override for bounded eviction.
     */
    @SuppressWarnings("java:S2160") // equals/hashCode not needed — internal cache, never compared
    static final class LruTermCache extends LinkedHashMap<Value, byte[]> {

        @Serial
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
        byte[] getOrCompute(Value term, Function<Value, byte[]> encoder) {
            return computeIfAbsent(term, encoder);
        }
    }

    /**
     * Abstract builder base for configuring serializer instances.
     *
     * @param <S> the concrete serializer type
     * @param <B> the concrete builder type (for fluent returns)
     */
    abstract static class AbstractBuilder<S extends AbstractFastRdfSerializer, B extends AbstractBuilder<S, B>> {

        int batchSize = DEFAULT_BATCH_SIZE;

        int cacheMaxSize = DEFAULT_CACHE_MAX_SIZE;

        AbstractBuilder() {}

        /**
         * Sets the number of statements to buffer before serializing as a batch.
         *
         * @param batchSize the batch size (must be positive)
         * @return this builder
         */
        public B batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be positive, got %d".formatted(batchSize));
            }
            this.batchSize = batchSize;
            return self();
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
