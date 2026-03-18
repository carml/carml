package io.carml.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XSD;

/**
 * Shared static encoding utilities for N-Triples and N-Quads serialization. Contains all
 * byte-level encoding logic: IRI encoding, blank node encoding, literal escaping, and term
 * writing.
 *
 * <p>Used by both {@link AbstractFastRdfSerializer} (streaming to OutputStream) and
 * {@link NTriplesTermEncoder} (encoding to byte arrays).
 *
 * <p>Package-private: internal implementation detail of the {@code io.carml.output} package.
 */
final class RdfTermEncoding {

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

    private RdfTermEncoding() {}

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
     * Writes a subject resource (IRI or blank node) to the output stream.
     *
     * @throws IllegalArgumentException if the resource is neither an IRI nor a BNode
     */
    static void writeResource(Resource resource, OutputStream output, LruTermCache termCache) {
        if (resource instanceof IRI iri) {
            writeIri(iri, output, termCache);
        } else if (resource instanceof BNode bNode) {
            writeBNode(bNode, output, termCache);
        } else {
            throw new IllegalArgumentException("Unsupported resource type: %s"
                    .formatted(resource.getClass().getName()));
        }
    }

    /**
     * Writes an object value (IRI, blank node, or literal) to the output stream.
     *
     * @throws IllegalArgumentException if the value is not an IRI, BNode, or Literal
     */
    static void writeValue(Value value, OutputStream output, LruTermCache termCache) {
        if (value instanceof IRI iri) {
            writeIri(iri, output, termCache);
        } else if (value instanceof BNode bNode) {
            writeBNode(bNode, output, termCache);
        } else if (value instanceof Literal literal) {
            writeLiteral(literal, output, termCache);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported value type: %s".formatted(value.getClass().getName()));
        }
    }

    private static void writeIri(IRI iri, OutputStream output, LruTermCache termCache) {
        var cached = termCache.getOrCompute(iri, RdfTermEncoding::encodeIri);
        writeBytes(cached, output);
    }

    private static void writeBNode(BNode bNode, OutputStream output, LruTermCache termCache) {
        var cached = termCache.getOrCompute(bNode, RdfTermEncoding::encodeBNode);
        writeBytes(cached, output);
    }

    private static void writeLiteral(Literal literal, OutputStream output, LruTermCache termCache) {
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
                                writeIri(datatype, output, termCache);
                            }
                        });
    }

    private static boolean isXsdString(IRI datatype) {
        return XSD.STRING.equals(datatype);
    }

    /**
     * Writes escaped literal string bytes directly to the output stream following the N-Triples
     * escaping rules (W3C N-Triples spec, section 4).
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

    static boolean isPrintableAscii(int codePoint) {
        return codePoint >= 0x0020 && codePoint <= 0x007E && codePoint != '"' && codePoint != '\\';
    }

    static void writeEscapedCodePoint(int codePoint, OutputStream output) {
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
    static void writeAsciiRun(String label, int start, int end, OutputStream output) {
        int runLength = end - start;
        var bytes = new byte[runLength];
        for (int j = 0; j < runLength; j++) {
            bytes[j] = (byte) label.charAt(start + j);
        }
        writeBytes(bytes, output);
    }

    static void writeUnicodeEscape4(int codePoint, OutputStream output) {
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

    static void writeUnicodeEscape8(int codePoint, OutputStream output) {
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
}
