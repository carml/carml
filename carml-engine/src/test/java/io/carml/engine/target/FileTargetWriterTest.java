package io.carml.engine.target;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.carml.output.RdfSerializer;
import io.carml.output.RdfSerializerFactory;
import io.carml.output.RdfSerializerProvider;
import io.carml.output.SerializerMode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FileTargetWriterTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final Statement TEST_STATEMENT = VF.createStatement(
            VF.createIRI("http://example.org/subject"), RDF.TYPE, VF.createIRI("http://example.org/Type"));

    @TempDir
    Path tempDir;

    @Test
    void open_write_close_writesNTriplesFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        // When
        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
        assertThat(content, containsString(RDF.TYPE.stringValue()));
        assertThat(content, containsString("<http://example.org/Type>"));
    }

    @Test
    void open_write_close_writesNQuadsFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nq");

        // When
        try (var writer = FileTargetWriter.builder().filePath(file).format("nq").build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void open_write_close_writesTurtleFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.ttl");

        // When
        try (var writer =
                FileTargetWriter.builder().filePath(file).format("ttl").build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("example.org"));
    }

    @Test
    void open_write_close_withGzipCompression_writesCompressedFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt.gz");
        var gzipIri = VF.createIRI(RML_NS, "gzip");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("nt")
                .compression(gzipIri)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        assertTrue(Files.exists(file));
        try (InputStream is = new GZIPInputStream(Files.newInputStream(file))) {
            var content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(content, containsString("<http://example.org/subject>"));
        }
    }

    @Test
    void open_write_close_withZipCompression_writesCompressedFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt.zip");
        var zipIri = VF.createIRI(RML_NS, "zip");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("nt")
                .compression(zipIri)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        assertTrue(Files.exists(file));
        try (var zis = new ZipArchiveInputStream(Files.newInputStream(file))) {
            zis.getNextEntry();
            var content = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(content, containsString("<http://example.org/subject>"));
        }
    }

    @Test
    void open_write_close_withUtf8Charset_writesUtf8File() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("nt")
                .charset(StandardCharsets.UTF_8)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file, StandardCharsets.UTF_8);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void open_write_close_withUtf16Charset_writesUtf16File() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("nt")
                .charset(StandardCharsets.UTF_16)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then - reading with UTF-16 must recover the original RDF content
        var content = Files.readString(file, StandardCharsets.UTF_16);
        assertThat(content, containsString("<http://example.org/subject>"));
        assertThat(content, containsString("<http://example.org/Type>"));

        // And - raw bytes must differ from the UTF-8 encoding of the same content, proving
        // transcoding was actually applied (UTF-16 uses 2+ bytes per code unit plus a BOM)
        var rawBytes = Files.readAllBytes(file);
        var utf8Bytes = content.getBytes(StandardCharsets.UTF_8);
        assertThat(rawBytes.length, not(equalTo(utf8Bytes.length)));
    }

    @Test
    void open_createsParentDirectories() throws IOException {
        // Given
        var file = tempDir.resolve("sub/dir/output.nt");

        // When
        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        assertTrue(Files.exists(file));
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void flush_doesNotThrowAndDoesNotCorruptOutput() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        // When
        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
            writer.flush();
        }

        // Then - verify output is valid after flush + close
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void open_withNoneCompression_writesUncompressedFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");
        var noneIri = VF.createIRI(RML_NS, "none");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("nt")
                .compression(noneIri)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"targz", "tarxz"})
    void open_withUnsupportedCompression_throwsUnsupportedOperationException(String compressionToken) {
        // Given - both targz and tarxz are rejected at open() time because archive compressions
        // cannot sensibly be produced in a single-stream write path
        var file = tempDir.resolve("output.nt." + compressionToken);
        var compressionIri = VF.createIRI(RML_NS, compressionToken);

        // When / Then
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("nt")
                .compression(compressionIri)
                .build()) {
            var exception = assertThrows(UnsupportedOperationException.class, writer::open);
            assertThat(exception.getMessage(), containsString("compression not supported for write"));
        }
    }

    @Test
    void open_write_close_withNullCompression_writesPlainFile() throws IOException {
        // Given - a builder with explicit compression(null) must write plain uncompressed output,
        // pinning the documented default behavior of the compression config field
        var file = tempDir.resolve("output.nt");

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("nt")
                .compression(null)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then - file is readable as plain UTF-8 text without any compression wrapper
        var content = Files.readString(file, StandardCharsets.UTF_8);
        assertThat(content, containsString("<http://example.org/subject>"));
        assertThat(content, containsString(RDF.TYPE.stringValue()));
    }

    @Test
    void open_whenAlreadyOpen_throwsIllegalStateException() {
        // Given - a writer that has already been opened
        var file = tempDir.resolve("output.nt");

        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            writer.open();

            // When / Then - a second open() call must fail fast with IllegalStateException,
            // guarding the documented single-open lifecycle contract
            var exception = assertThrows(IllegalStateException.class, writer::open);
            assertThat(exception.getMessage(), containsString("already open"));
        }
    }

    @Test
    void write_multipleStatements_allWrittenToFile() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");

        var statement1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/T1"));

        var statement2 = VF.createStatement(
                VF.createIRI("http://example.org/s2"), RDF.TYPE, VF.createIRI("http://example.org/T2"));

        // When
        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            writer.open();
            writer.write(statement1);
            writer.write(statement2);
        }

        // Then
        var content = Files.readString(file);
        assertThat(content, containsString("http://example.org/s1"));
        assertThat(content, containsString("http://example.org/s2"));
    }

    @Test
    void open_invalidPath_throwsUncheckedIOException() {
        // Given
        var file = Path.of("/nonexistent/readonly/path/output.nt");

        // When/Then
        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            var exception = assertThrows(UncheckedIOException.class, writer::open);
            assertThat(exception.getMessage(), containsString("Failed to open file target"));
        }
    }

    @Test
    void open_write_close_withStubSerializer_invokesLifecycleInOrder() {
        // Given
        var file = tempDir.resolve("output.custom");
        var stub = new RecordingSerializer();
        var factory = RdfSerializerFactory.of(List.of(new StubProvider("custom", stub)));

        // When
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("custom")
                .serializerFactory(factory)
                .namespaces(Map.of("ex", "http://example.org/"))
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
            writer.flush();
        }

        // Then
        assertThat(stub.events, equalTo(List.of("start", "write", "flush", "end", "close")));
        assertThat(stub.namespaces, equalTo(Map.of("ex", "http://example.org/")));
    }

    @Test
    void close_withoutOpen_doesNotThrow() {
        // Given
        var file = tempDir.resolve("output.nt");
        var writer = FileTargetWriter.builder().filePath(file).format("nt").build();

        // When / Then - close without open must be safe (open may have failed mid-way)
        writer.close();
    }

    @Test
    void close_calledTwice_doesNotThrow() throws IOException {
        // Given
        var file = tempDir.resolve("output.nt");
        var writer = FileTargetWriter.builder().filePath(file).format("nt").build();
        writer.open();
        writer.write(TEST_STATEMENT);

        // When
        writer.close();
        writer.close();

        // Then - output is still valid and second close is a no-op
        var content = Files.readString(file);
        assertThat(content, containsString("<http://example.org/subject>"));
    }

    @Test
    void write_withoutOpen_throwsIllegalStateException() {
        // Given
        var file = tempDir.resolve("output.nt");
        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            // When / Then
            var exception = assertThrows(IllegalStateException.class, () -> writer.write(TEST_STATEMENT));
            assertThat(exception.getMessage(), containsString("not open or already closed"));
        }
    }

    @Test
    void write_afterClose_throwsIllegalStateException() {
        // Given
        var file = tempDir.resolve("output.nt");
        var writer = FileTargetWriter.builder().filePath(file).format("nt").build();
        writer.open();
        writer.write(TEST_STATEMENT);
        writer.close();

        // When / Then
        assertThrows(IllegalStateException.class, () -> writer.write(TEST_STATEMENT));
    }

    @Test
    void flush_withoutOpen_throwsIllegalStateException() {
        // Given
        var file = tempDir.resolve("output.nt");
        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            // When / Then - flush before open is a usage error, symmetric with write(Statement)
            var exception = assertThrows(IllegalStateException.class, writer::flush);
            assertThat(exception.getMessage(), containsString("not open or already closed"));
        }
    }

    @Test
    void flush_afterClose_throwsIllegalStateException() {
        // Given
        var file = tempDir.resolve("output.nt");
        var writer = FileTargetWriter.builder().filePath(file).format("nt").build();
        writer.open();
        writer.close();

        // When / Then
        assertThrows(IllegalStateException.class, writer::flush);
    }

    @Test
    void close_whenSerializerEndThrows_propagatesAndStillClosesStream() {
        // Given - a stub serializer whose end() throws. The invariants under test:
        //   1. The exception from end() propagates out of close().
        //   2. The underlying output stream is still closed (endSerializerThenCloseChain's finally
        //      block). Verified indirectly: the file handle must be releasable for a subsequent
        //      reopen-and-write to succeed on all platforms.
        //   3. A subsequent close() is a no-op (serializer and outputStream nulled out).
        var file = tempDir.resolve("output.custom");
        var stub = new EndFailingSerializer();
        var factory = RdfSerializerFactory.of(List.of(new StubProvider("custom", stub)));

        var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("custom")
                .serializerFactory(factory)
                .build();
        writer.open();

        // When / Then - exception propagates
        var exception = assertThrows(RuntimeException.class, writer::close);
        assertThat(exception.getMessage(), containsString("end-failure"));

        // And - a second close must be a no-op (both fields are already nulled out)
        writer.close();

        // And - the file handle must have been released: a fresh writer can reopen and overwrite.
        // If the stream chain had leaked, this reopen would fail on Windows and would leave a
        // handle dangling on POSIX.
        try (var reopen = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            reopen.open();
            reopen.write(TEST_STATEMENT);
        }
        assertTrue(Files.exists(file));
    }

    @Test
    void close_whenEndAndOutputStreamCloseBothThrow_propagatesEndExceptionAndNullsFields() {
        // Given - a serializer whose end() throws AND an outputStream whose close() throws.
        // The invariants under test:
        //   1. The end() exception propagates out of close() as the PRIMARY failure (thrown from the
        //      outer try in endSerializerThenCloseChain).
        //   2. The outputStream.close() IOException does NOT mask the primary failure — it is
        //      caught-and-logged inside closeOutputStream(), as documented.
        //   3. Fields are nulled so a subsequent close() is a no-op.
        var file = tempDir.resolve("output.custom");
        var stub = new EndFailingSerializer();
        var factory = RdfSerializerFactory.of(List.of(new StubProvider("custom", stub)));

        // A FileTargetWriter subclass that replaces the charset wrapper with a close-failing wrapper.
        // This is the only injection point that lets us substitute the outputStream without changing
        // the production lifecycle path. The applyCharset hook is package-private specifically for
        // this test (see Javadoc in FileTargetWriter#applyCharset).
        var writer = new FileTargetWriter(file, "custom", SerializerMode.STREAMING, factory, Map.of(), null, null) {
            @Override
            OutputStream applyCharset(OutputStream compressed) {
                return new CloseFailingOutputStream(compressed);
            }
        };
        writer.open();

        // When / Then - the end() exception propagates; the outputStream close IOException is
        // swallowed-and-logged
        var exception = assertThrows(RuntimeException.class, writer::close);
        assertThat(exception.getMessage(), containsString("end-failure"));

        // And - the primary exception's suppressed array is empty: the IOException from
        // outputStream.close() is logged rather than attached, per FileTargetWriter's documented
        // contract (see closeOutputStream)
        assertThat(exception.getSuppressed().length, equalTo(0));

        // And - a second close must be a no-op (serializer and outputStream nulled out)
        writer.close();
    }

    @Test
    void open_whenSerializerCreationFails_releasesStreamAndRethrows() {
        // Given - a factory that rejects the requested format so createSerializer throws
        var file = tempDir.resolve("output.custom");
        var factory = RdfSerializerFactory.of(List.of());

        // When / Then
        try (var writer = FileTargetWriter.builder()
                .filePath(file)
                .format("custom")
                .serializerFactory(factory)
                .build()) {
            assertThrows(IllegalArgumentException.class, writer::open);
        }

        // And - a follow-up reopen with a valid factory must succeed, proving the file handle from
        // the failed attempt was released (otherwise Windows file locks would prevent re-use)
        try (var writer = FileTargetWriter.builder().filePath(file).format("nt").build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }
        assertTrue(Files.exists(file));
    }

    /**
     * Test-only provider that returns a fixed serializer for a fixed format token, in any
     * {@link SerializerMode}.
     */
    private record StubProvider(String format, RdfSerializer serializer) implements RdfSerializerProvider {

        @Override
        public boolean supports(String requestedFormat, SerializerMode mode) {
            return format.equals(requestedFormat) && mode != null;
        }

        @Override
        public int priority() {
            return 1000;
        }

        @Override
        public RdfSerializer createSerializer(String requestedFormat, SerializerMode mode) {
            return serializer;
        }
    }

    /**
     * Test-only serializer that records its lifecycle invocations.
     */
    private static final class RecordingSerializer implements RdfSerializer {

        private final List<String> events = new ArrayList<>();

        private Map<String, String> namespaces;

        private OutputStream output;

        @Override
        public void start(OutputStream output, Map<String, String> namespaces) {
            this.output = output;
            this.namespaces = Map.copyOf(namespaces);
            events.add("start");
        }

        @Override
        public void write(Statement statement) {
            try {
                output.write("#stmt\n".getBytes(StandardCharsets.UTF_8));
            } catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            }
            events.add("write");
        }

        @Override
        public void end() {
            events.add("end");
        }

        @Override
        public void flush() {
            events.add("flush");
        }

        @Override
        public void close() {
            events.add("close");
        }
    }

    /**
     * Test-only serializer whose {@link #end()} throws a {@link RuntimeException}. No-ops on all
     * other lifecycle methods. Used to pin the {@code endSerializerThenCloseChain} invariant that
     * {@link FileTargetWriter#close()} still closes the stream chain even if {@code end()} fails.
     */
    private static final class EndFailingSerializer implements RdfSerializer {

        @Override
        public void start(OutputStream output, Map<String, String> namespaces) {
            // no-op
        }

        @Override
        public void write(Statement statement) {
            // no-op
        }

        @Override
        public void end() {
            throw new RuntimeException("end-failure");
        }

        @Override
        public void flush() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * Test-only output stream that delegates writes to the underlying stream but throws an
     * {@link IOException} on {@link #close()}. Used to pin the contract that a secondary close
     * failure is swallowed-and-logged rather than masking a primary serializer-end failure.
     */
    private static final class CloseFailingOutputStream extends OutputStream {

        private final OutputStream delegate;

        private CloseFailingOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int byteValue) throws IOException {
            delegate.write(byteValue);
        }

        @Override
        public void write(byte @NonNull [] bytes, int off, int len) throws IOException {
            delegate.write(bytes, off, len);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            // Release the underlying stream so the file handle is not leaked, then surface the
            // secondary failure to exercise FileTargetWriter's swallow-and-log contract.
            delegate.close();
            throw new IOException("close-failure");
        }
    }
}
