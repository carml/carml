package io.carml.engine.target;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.output.RdfSerializer;
import io.carml.output.RdfSerializerFactory;
import io.carml.output.RdfSerializerProvider;
import io.carml.output.SerializerMode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

class StreamTargetWriterTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static final Statement TEST_STATEMENT = VF.createStatement(
            VF.createIRI("http://example.org/subject"), RDF.TYPE, VF.createIRI("http://example.org/Type"));

    @Test
    void openWriteClose_writesNTriplesToStream_andLeavesStreamOpen() throws IOException {
        // Given
        var tracker = new CloseTrackingOutputStream(new ByteArrayOutputStream());

        // When
        try (var writer =
                StreamTargetWriter.builder().outputStream(tracker).format("nt").build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then — content written, underlying stream is NOT closed.
        var content = ((ByteArrayOutputStream) tracker.delegate).toString(StandardCharsets.UTF_8);
        assertThat(content, containsString("<http://example.org/subject>"));
        assertThat(tracker.closeCount, is(0));
    }

    @Test
    void write_withoutOpen_throwsIllegalStateException() {
        // Given
        try (var writer = StreamTargetWriter.builder()
                .outputStream(new ByteArrayOutputStream())
                .format("nt")
                .build()) {
            // When / Then
            var exception = assertThrows(IllegalStateException.class, () -> writer.write(TEST_STATEMENT));
            assertThat(exception.getMessage(), containsString("not open or already closed"));
        }
    }

    @Test
    void open_calledTwice_throwsIllegalStateException() {
        // Given
        try (var writer = StreamTargetWriter.builder()
                .outputStream(new ByteArrayOutputStream())
                .format("nt")
                .build()) {
            writer.open();

            // When / Then
            var exception = assertThrows(IllegalStateException.class, writer::open);
            assertThat(exception.getMessage(), containsString("already open"));
        }
    }

    @Test
    void close_calledTwice_isIdempotent() {
        // Given
        var writer = StreamTargetWriter.builder()
                .outputStream(new ByteArrayOutputStream())
                .format("nt")
                .build();
        writer.open();

        // When
        writer.close();
        writer.close();

        // Then — no exception; subsequent write must fail because writer is closed
        assertThrows(IllegalStateException.class, () -> writer.write(TEST_STATEMENT));
    }

    @Test
    void open_whenSerializerCreationFails_cleansUpAndDoesNotCloseUnderlyingStream() {
        // Given — a factory that throws RuntimeException from createSerializer exercises the
        // partial-open cleanup path. The invariant: the caller's stream is left open.
        var tracker = new CloseTrackingOutputStream(new ByteArrayOutputStream());
        var factory = RdfSerializerFactory.of(List.of(new ThrowingProvider("custom")));

        var writer = StreamTargetWriter.builder()
                .outputStream(tracker)
                .format("custom")
                .serializerFactory(factory)
                .build();

        // When / Then
        var exception = assertThrows(RuntimeException.class, writer::open);
        assertThat(exception.getMessage(), containsString("boom"));
        assertThat(tracker.closeCount, is(0));

        // And — a subsequent close() on the writer is still a no-op (serializer was never set)
        writer.close();
        assertThat(tracker.closeCount, is(0));
    }

    @Test
    void namespaces_arePassedToSerializer() {
        // Given
        var stub = new RecordingSerializer();
        var factory = RdfSerializerFactory.of(List.of(new StubProvider("custom", stub)));
        var namespaces = Map.of("ex", "http://example.org/");

        // When
        try (var writer = StreamTargetWriter.builder()
                .outputStream(new ByteArrayOutputStream())
                .format("custom")
                .serializerFactory(factory)
                .namespaces(namespaces)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
            writer.flush();
        }

        // Then
        assertThat(stub.events, equalTo(List.of("start", "write", "flush", "end", "close")));
        assertThat(stub.namespaces, equalTo(namespaces));
    }

    @Test
    void close_doesNotCloseUnderlyingStream_evenAfterSerializerEndThrows() {
        // Given — a serializer whose end() throws. The invariants under test:
        //   1. The exception from end() propagates out of close().
        //   2. The caller-provided output stream is STILL open — caller ownership is the
        //      defining invariant of StreamTargetWriter.
        var tracker = new CloseTrackingOutputStream(new ByteArrayOutputStream());
        var stub = new EndFailingSerializer();
        var factory = RdfSerializerFactory.of(List.of(new StubProvider("custom", stub)));

        var writer = StreamTargetWriter.builder()
                .outputStream(tracker)
                .format("custom")
                .serializerFactory(factory)
                .build();
        writer.open();

        // When / Then
        var exception = assertThrows(RuntimeException.class, writer::close);
        assertThat(exception.getMessage(), containsString("end-failure"));
        assertThat(tracker.closeCount, is(0));
    }

    @Test
    void flush_afterClose_throwsIllegalStateException() {
        // Given
        var writer = StreamTargetWriter.builder()
                .outputStream(new ByteArrayOutputStream())
                .format("nt")
                .build();
        writer.open();
        writer.close();

        // When / Then
        assertThrows(IllegalStateException.class, writer::flush);
    }

    @Test
    @SuppressWarnings(
            "resource") // ExecutorService.close() is Java 19+; project compiles to 17 — shutdownNow in finally
    void write_concurrentInvocations_serializesWritesWithoutLoss() throws InterruptedException {
        // Given — the TargetRouter dispatches from multiple reactive threads when the engine
        // evaluates TriplesMaps in parallel. Concurrent writes must be serialized by the writer
        // (see class-level Javadoc). This test gates N threads behind a CountDownLatch, then
        // verifies the serializer received exactly N writes with no exceptions and no loss.
        var recording = new RecordingSerializer();
        var factory = RdfSerializerFactory.of(List.of(new StubProvider("custom", recording)));
        int threads = 16;
        var executor = Executors.newFixedThreadPool(threads);

        try (var writer = StreamTargetWriter.builder()
                .outputStream(new ByteArrayOutputStream())
                .format("custom")
                .serializerFactory(factory)
                .build()) {
            writer.open();

            var startGate = new CountDownLatch(1);
            var finishGate = new CountDownLatch(threads);
            var failures = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        startGate.await();
                        writer.write(TEST_STATEMENT);
                    } catch (RuntimeException | InterruptedException ex) {
                        failures.incrementAndGet();
                    } finally {
                        finishGate.countDown();
                    }
                });
            }

            startGate.countDown();
            assertThat(finishGate.await(5, TimeUnit.SECONDS), is(true));

            // Then
            assertThat(failures.get(), is(0));
            assertThat(recording.writeCount.get(), is(threads));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void openWriteClose_withUtf16Charset_writesUtf16EncodedBytes() {
        // Given — serializers always emit UTF-8; with a non-UTF-8 charset configured,
        // StreamTargetWriter installs a transcoding wrapper so the caller's OutputStream receives
        // bytes in the requested encoding. This mirrors FileTargetWriter's charset test.
        var sink = new ByteArrayOutputStream();

        // When
        try (var writer = StreamTargetWriter.builder()
                .outputStream(sink)
                .format("nt")
                .charset(StandardCharsets.UTF_16)
                .build()) {
            writer.open();
            writer.write(TEST_STATEMENT);
        }

        // Then — decoding the bytes with UTF-16 must recover the original RDF content.
        var utf16Content = sink.toString(StandardCharsets.UTF_16);
        assertThat(utf16Content, containsString("<http://example.org/subject>"));
        assertThat(utf16Content, containsString("<http://example.org/Type>"));

        // And — raw byte length differs from the UTF-8 encoding of the same content, proving
        // transcoding was actually applied (UTF-16 uses 2+ bytes per code unit plus a BOM).
        var rawLength = sink.size();
        var utf8Length = utf16Content.getBytes(StandardCharsets.UTF_8).length;
        assertThat(rawLength, not(equalTo(utf8Length)));
    }

    /**
     * Test-only {@link RdfSerializerProvider} whose {@link #createSerializer} throws. Used to
     * pin the partial-open cleanup invariant: a failed open() must not close the caller's
     * stream.
     */
    private record ThrowingProvider(String format) implements RdfSerializerProvider {

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
            throw new RuntimeException("boom");
        }
    }

    /**
     * Test-only {@link RdfSerializerProvider} that returns a caller-supplied serializer
     * regardless of the requested format/mode combination, scoped to a single format token.
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
     * Test-only serializer that records its lifecycle invocations and the namespace map. The
     * {@code writeCount} counter is atomic because the concurrency test fires writes from
     * multiple threads; the {@link StreamTargetWriter} serializes them via {@code synchronized},
     * but we still read the counter without holding that lock.
     */
    private static final class RecordingSerializer implements RdfSerializer {

        private final List<String> events = new ArrayList<>();

        private final AtomicInteger writeCount = new AtomicInteger();

        private Map<String, String> namespaces;

        @Override
        public void start(OutputStream output, Map<String, String> namespaces) {
            this.namespaces = Map.copyOf(namespaces);
            events.add("start");
        }

        @Override
        public void write(Statement statement) {
            events.add("write");
            writeCount.incrementAndGet();
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
     * Test-only serializer whose {@link #end()} throws a {@link RuntimeException}. All other
     * lifecycle methods are no-ops.
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
     * Test-only {@link OutputStream} that counts {@link #close()} invocations. Used to pin the
     * "does not close caller-owned stream" invariant.
     */
    private static final class CloseTrackingOutputStream extends OutputStream {

        private final OutputStream delegate;

        private int closeCount;

        private CloseTrackingOutputStream(OutputStream delegate) {
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
            closeCount++;
            delegate.close();
        }
    }
}
