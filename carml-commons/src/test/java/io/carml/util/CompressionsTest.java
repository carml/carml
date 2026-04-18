package io.carml.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CompressionsTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final IRI TARGZ = VF.createIRI(RML_NS, "targz");

    private static final IRI TARXZ = VF.createIRI(RML_NS, "tarxz");

    private static final IRI ZIP = VF.createIRI(RML_NS, "zip");

    private static final IRI GZIP = VF.createIRI(RML_NS, "gzip");

    private static final IRI NONE = VF.createIRI(RML_NS, "none");

    private static final String PAYLOAD = "hello,carml,compression\n";

    private static Stream<Arguments> validDecompression() {
        return Stream.of(
                Arguments.of("input.tar.gz", TARGZ),
                Arguments.of("input.tar.xz", TARXZ),
                Arguments.of("input.zip", ZIP),
                Arguments.of("input.csv.gz", GZIP),
                Arguments.of("input.csv", NONE),
                Arguments.of("input.csv", null),
                Arguments.of("input.csv", VF.createIRI(RML_NS, "unknown")));
    }

    @ParameterizedTest
    @MethodSource("validDecompression")
    void givenInputAndCompression_whenDecompress_thenReturnExpectedResult(String inputFileName, IRI compression)
            throws IOException {
        // Given
        var compressedInput = CompressionsTest.class.getResourceAsStream(inputFileName);

        // When
        var decompressed = Compressions.decompress(compressedInput, compression);

        // Then
        assertThat(IOUtils.toString(decompressed, StandardCharsets.UTF_8), startsWith("foo,bar,baz"));
    }

    private static Stream<Arguments> invalidDecompression() {
        return Stream.of(
                Arguments.of("input.tar.gz", TARXZ),
                Arguments.of("input.tar.xz", TARGZ),
                Arguments.of("input.zip", GZIP),
                Arguments.of("input.csv.gz", ZIP));
    }

    @ParameterizedTest
    @MethodSource("invalidDecompression")
    void givenInputAndInvalidCompression_whenDecompress_thenThrowException(String inputFileName, IRI compression)
            throws IOException {
        // Given
        try (var compressedInput = CompressionsTest.class.getResourceAsStream(inputFileName)) {
            // When
            var exception = assertThrows(
                    UncheckedIOException.class, () -> Compressions.decompress(compressedInput, compression));

            // Then
            assertThat(exception.getMessage(), startsWith("Could not open"));
        }
    }

    private static Stream<Arguments> roundTripCompression() {
        return Stream.of(Arguments.of(GZIP), Arguments.of(ZIP));
    }

    @ParameterizedTest
    @MethodSource("roundTripCompression")
    void givenPayloadAndCompression_whenCompressThenDecompress_thenRoundTripsCleanly(IRI compression)
            throws IOException {
        // Given
        var buffer = new ByteArrayOutputStream();

        // When
        try (var compressed = Compressions.compress(buffer, compression, "entry.csv")) {
            compressed.write(PAYLOAD.getBytes(StandardCharsets.UTF_8));
        }

        var compressedBytes = buffer.toByteArray();
        String roundTripped;
        try (var decompressed = Compressions.decompress(new ByteArrayInputStream(compressedBytes), compression)) {
            roundTripped = IOUtils.toString(decompressed, StandardCharsets.UTF_8);
        }

        // Then
        assertThat(roundTripped, equalTo(PAYLOAD));
    }

    @Test
    void givenNullCompression_whenCompress_thenReturnsOriginalStream() {
        // Given
        var buffer = new ByteArrayOutputStream();

        // When
        var compressed = Compressions.compress(buffer, null, "ignored");

        // Then
        assertEquals(buffer, compressed);
    }

    @Test
    void givenNoneCompression_whenCompress_thenReturnsOriginalStream() {
        // Given
        var buffer = new ByteArrayOutputStream();

        // When
        var compressed = Compressions.compress(buffer, NONE, "ignored");

        // Then
        assertEquals(buffer, compressed);
    }

    @Test
    void givenUnknownCompression_whenCompress_thenReturnsOriginalStream() {
        // Given
        var buffer = new ByteArrayOutputStream();
        var unknown = VF.createIRI(RML_NS, "unknown");

        // When
        var compressed = Compressions.compress(buffer, unknown, "ignored");

        // Then
        assertEquals(buffer, compressed);
    }

    private static Stream<Arguments> unsupportedForWriteCompression() {
        return Stream.of(Arguments.of(TARGZ, "targz"), Arguments.of(TARXZ, "tarxz"));
    }

    @ParameterizedTest
    @MethodSource("unsupportedForWriteCompression")
    void givenArchiveCompression_whenCompress_thenThrowsUnsupportedOperationException(IRI compression, String token) {
        // Given
        var buffer = new ByteArrayOutputStream();

        // When
        var exception = assertThrows(
                UnsupportedOperationException.class, () -> Compressions.compress(buffer, compression, "entry"));

        // Then
        assertThat(exception.getMessage(), equalTo("compression not supported for write: " + RML_NS + token));
    }

    private static Stream<Arguments> ioFailingCompression() {
        return Stream.of(
                Arguments.of(GZIP, "Could not open gzip output"), Arguments.of(ZIP, "Could not open zip output"));
    }

    @ParameterizedTest
    @MethodSource("ioFailingCompression")
    void givenFailingStream_whenCompress_thenWrapsInUncheckedIoException(IRI compression, String expectedPrefix)
            throws IOException {
        // Given
        try (var failing = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("boom");
            }

            @Override
            public void write(byte @NonNull [] bytes, int off, int len) throws IOException {
                throw new IOException("boom");
            }

            @Override
            public void flush() throws IOException {
                throw new IOException("boom");
            }
        }) {
            // When
            var exception = assertThrows(
                    UncheckedIOException.class, () -> Compressions.compress(failing, compression, "entry.csv"));

            // Then
            assertThat(exception.getMessage(), startsWith(expectedPrefix));
        }
    }

    @Test
    void compress_zipStream_closeTwice_doesNotThrow() throws IOException {
        // Given
        var buffer = new ByteArrayOutputStream();
        var zipStream = Compressions.compress(buffer, ZIP, "entry.csv");
        zipStream.write(PAYLOAD.getBytes(StandardCharsets.UTF_8));

        // When - first close finalizes the archive entry and closes the zip stream; second close
        // is a no-op guarded by the closed flag on ZipEntryClosingOutputStream
        zipStream.close();
        zipStream.close();

        // Then - output is a valid zip archive that round-trips cleanly
        try (var decompressed = Compressions.decompress(new ByteArrayInputStream(buffer.toByteArray()), ZIP)) {
            assertThat(IOUtils.toString(decompressed, StandardCharsets.UTF_8), equalTo(PAYLOAD));
        }
    }

    @Test
    void compress_zipStream_writeSingleByte_isWrittenCorrectly() throws IOException {
        // Given - exercises the single-byte write(int) override that bypasses FilterOutputStream's
        // default per-byte indirection and delegates directly to the underlying zip stream
        var buffer = new ByteArrayOutputStream();
        var payload = "abc".getBytes(StandardCharsets.UTF_8);

        // When - write each byte individually via write(int)
        try (var zipStream = Compressions.compress(buffer, ZIP, "entry.csv")) {
            for (byte byteValue : payload) {
                zipStream.write(byteValue & 0xFF);
            }
        }

        // Then - each byte round-trips through compression/decompression intact
        String roundTripped;
        try (var decompressed = Compressions.decompress(new ByteArrayInputStream(buffer.toByteArray()), ZIP)) {
            roundTripped = IOUtils.toString(decompressed, StandardCharsets.UTF_8);
        }
        assertThat(roundTripped, equalTo("abc"));
    }
}
