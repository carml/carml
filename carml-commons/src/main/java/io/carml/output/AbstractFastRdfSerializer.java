package io.carml.output;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serial;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
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
 * Abstract base class for high-throughput RDF serializers (N-Triples, N-Quads).
 *
 * <p>Contains all shared serialization logic: direct stream output, term encoding, literal
 * escaping, LRU caching, and the builder pattern. Subclasses only need to implement
 * {@link #writeStatement(Statement, OutputStream)} to define the per-statement output format.
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

    static final int DEFAULT_CACHE_MAX_SIZE = 65536;

    /** Buffer size for {@link BufferedOutputStream} wrapping the caller's output stream. */
    private static final int OUTPUT_BUFFER_SIZE = 65536;

    private final LruTermCache termCache;

    AbstractFastRdfSerializer(int cacheMaxSize) {
        this.termCache = new LruTermCache(cacheMaxSize);
    }

    /**
     * Serializes a {@link Flux} of {@link Statement}s to the provided {@link OutputStream}.
     * Statements are written directly through a {@link BufferedOutputStream} for throughput.
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
        if (resource instanceof IRI iri) {
            writeIri(iri, output);
        } else if (resource instanceof BNode bNode) {
            writeBNode(bNode, output);
        }
    }

    /**
     * Writes an object value (IRI, blank node, or literal) to the output stream.
     */
    void writeValue(Value value, OutputStream output) {
        if (value instanceof IRI iri) {
            writeIri(iri, output);
        } else if (value instanceof BNode bNode) {
            writeBNode(bNode, output);
        } else if (value instanceof Literal literal) {
            writeLiteral(literal, output);
        }
    }

    private void writeIri(IRI iri, OutputStream output) {
        var cached = termCache.getOrCompute(iri, AbstractFastRdfSerializer::encodeIri);
        writeBytes(cached, output);
    }

    private void writeBNode(BNode bNode, OutputStream output) {
        var cached = termCache.getOrCompute(bNode, AbstractFastRdfSerializer::encodeBNode);
        writeBytes(cached, output);
    }

    private void writeLiteral(Literal literal, OutputStream output) {
        writeBytes(QUOTE, output);
        writeEscapedLiteralBytes(literal.getLabel(), output);
        writeBytes(QUOTE, output);

        literal.getLanguage()
                .ifPresentOrElse(
                        language -> {
                            writeBytes(LANG_TAG_PREFIX, output);
                            writeBytes(language.getBytes(StandardCharsets.UTF_8), output);
                        },
                        () -> {
                            var datatype = literal.getDatatype();
                            if (datatype != null && !isXsdString(datatype)) {
                                writeBytes(DATATYPE_PREFIX, output);
                                writeIri(datatype, output);
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
     * Writes escaped literal string bytes directly to the output stream following the N-Triples
     * escaping rules (W3C N-Triples spec, section 4):
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
     *
     * <p>Uses streaming codepoint iteration via {@link String#codePointAt(int)} to avoid allocating
     * an intermediate {@code int[]} array. Consecutive printable ASCII characters are batched into
     * a single write call to minimize virtual dispatch overhead. Unicode escapes are written as
     * single pre-built byte arrays to avoid per-byte write overhead.
     */
    static void writeEscapedLiteralBytes(String label, OutputStream output) {
        int len = label.length();
        int asciiRunStart = -1;
        int i = 0;

        while (i < len) {
            int codePoint = label.codePointAt(i);
            int charCount = Character.charCount(codePoint);

            if (isPrintableAscii(codePoint)) {
                if (asciiRunStart < 0) {
                    asciiRunStart = i;
                }
                i += charCount;
                continue;
            }

            if (asciiRunStart >= 0) {
                writeAsciiRun(label, asciiRunStart, i, output);
                asciiRunStart = -1;
            }

            writeEscapedCodePoint(codePoint, output);
            i += charCount;
        }

        if (asciiRunStart >= 0) {
            writeAsciiRun(label, asciiRunStart, len, output);
        }
    }

    private static boolean isPrintableAscii(int codePoint) {
        return codePoint >= 0x0020 && codePoint <= 0x007E && codePoint != '"' && codePoint != '\\';
    }

    private static void writeEscapedCodePoint(int codePoint, OutputStream output) {
        switch (codePoint) {
            case '\b' -> writeBytes(ESCAPE_BACKSPACE, output);
            case '\t' -> writeBytes(ESCAPE_TAB, output);
            case '\n' -> writeBytes(ESCAPE_NEWLINE, output);
            case '\f' -> writeBytes(ESCAPE_FORMFEED, output);
            case '\r' -> writeBytes(ESCAPE_CR, output);
            case '"' -> writeBytes(ESCAPE_QUOTE, output);
            case '\\' -> writeBytes(ESCAPE_BACKSLASH, output);
            default -> writeUnicodeEscape(codePoint, output);
        }
    }

    private static void writeUnicodeEscape(int codePoint, OutputStream output) {
        if (codePoint <= 0xFFFF) {
            writeUnicodeEscape4(codePoint, output);
        } else {
            writeUnicodeEscape8(codePoint, output);
        }
    }

    /**
     * Writes a run of printable ASCII characters from the label string as raw bytes. Since all
     * characters in the run are in the ASCII range (0x0020-0x007E), each char maps to exactly one
     * byte.
     */
    private static void writeAsciiRun(String label, int start, int end, OutputStream output) {
        int runLength = end - start;
        var bytes = new byte[runLength];
        for (int j = 0; j < runLength; j++) {
            bytes[j] = (byte) label.charAt(start + j);
        }
        writeBytes(bytes, output);
    }

    private static void writeUnicodeEscape4(int codePoint, OutputStream output) {
        // Writes backslash-uXXXX as a single 6-byte array -- avoids per-byte virtual dispatch
        writeBytes(
                new byte[] {
                    '\\',
                    'u',
                    (byte) HEX_DIGITS[(codePoint >> 12) & 0xF],
                    (byte) HEX_DIGITS[(codePoint >> 8) & 0xF],
                    (byte) HEX_DIGITS[(codePoint >> 4) & 0xF],
                    (byte) HEX_DIGITS[codePoint & 0xF]
                },
                output);
    }

    private static void writeUnicodeEscape8(int codePoint, OutputStream output) {
        // Writes backslash-UXXXXXXXX as a single 10-byte array -- avoids per-byte virtual dispatch
        writeBytes(
                new byte[] {
                    '\\',
                    'U',
                    (byte) HEX_DIGITS[(codePoint >> 28) & 0xF],
                    (byte) HEX_DIGITS[(codePoint >> 24) & 0xF],
                    (byte) HEX_DIGITS[(codePoint >> 20) & 0xF],
                    (byte) HEX_DIGITS[(codePoint >> 16) & 0xF],
                    (byte) HEX_DIGITS[(codePoint >> 12) & 0xF],
                    (byte) HEX_DIGITS[(codePoint >> 8) & 0xF],
                    (byte) HEX_DIGITS[(codePoint >> 4) & 0xF],
                    (byte) HEX_DIGITS[codePoint & 0xF]
                },
                output);
    }

    static void writeBytes(byte[] bytes, OutputStream output) {
        try {
            output.write(bytes, 0, bytes.length);
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
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

        int cacheMaxSize = DEFAULT_CACHE_MAX_SIZE;

        AbstractBuilder() {}

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
