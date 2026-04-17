package io.carml.output.jena;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.output.RdfSerializationException;
import io.carml.output.RdfSerializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.jena.riot.Lang;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link RdfSerializer} lifecycle contract for the Jena-backed serializers
 * ({@link JenaStreamingSerializer} and {@link JenaModelSerializer}). Mirrors the Rio lifecycle
 * test structure to ensure both SPI families behave consistently.
 */
class JenaSerializerLifecycleTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    // ---- streaming: start / write / end produces parseable output ----

    @Test
    void lifecycle_streamingNTriples_producesNTriplesOutput() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new JenaStreamingSerializer(Lang.NTRIPLES)) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(
                result,
                containsString("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing>"));
    }

    @Test
    void lifecycle_streamingTurtleSingleStatement_producesTurtleOutput() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new JenaStreamingSerializer(Lang.TURTLE)) {
            serializer.start(output, Map.of("ex", "http://example.org/"));
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(result, containsString("ex:"));
    }

    // ---- pretty: buffers until end, then emits complete document ----

    @Test
    void lifecycle_prettyTurtle_emitsOnEnd() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new JenaModelSerializer(Lang.TURTLE)) {
            serializer.start(output, Map.of("ex", "http://example.org/"));
            serializer.write(statement);
            // Before end() the output stream should still be empty - pretty mode buffers.
            assertThat(output.size(), is(0));
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(result, containsString("ex:s"));
    }

    @Test
    void lifecycle_prettyTurtleMultipleStatements_emitsAll() {
        var output = new ByteArrayOutputStream();
        var stmt1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));
        var stmt2 = VF.createStatement(VF.createIRI("http://example.org/s2"), RDFS.LABEL, VF.createLiteral("hello"));

        try (var serializer = new JenaModelSerializer(Lang.TURTLE)) {
            serializer.start(output, Map.of());
            serializer.write(stmt1);
            serializer.write(stmt2);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(result, containsString("http://example.org/s1"));
        assertThat(result, containsString("http://example.org/s2"));
        assertThat(result, containsString("hello"));
    }

    // ---- write() guards: streaming ----

    @Test
    void write_beforeStartStreaming_throwsIllegalStateException() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new JenaStreamingSerializer(Lang.NTRIPLES)) {
            var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
            assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
        }
    }

    @Test
    void write_afterEndStreaming_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new JenaStreamingSerializer(Lang.NTRIPLES)) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();

            var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
            assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
        }
    }

    @Test
    void write_afterCloseStreaming_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var serializer = new JenaStreamingSerializer(Lang.NTRIPLES);
        serializer.start(output, Map.of());
        serializer.write(statement);
        serializer.end();
        serializer.close();

        var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
        assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
    }

    @Test
    void start_calledTwiceStreaming_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var secondOutput = new ByteArrayOutputStream();
        var emptyMap = Map.<String, String>of();

        try (var serializer = new JenaStreamingSerializer(Lang.NTRIPLES)) {
            serializer.start(output, Map.of());

            var thrown = assertThrows(IllegalStateException.class, () -> serializer.start(secondOutput, emptyMap));
            assertThat(thrown.getMessage(), is("start() called while a session is already active"));

            serializer.end();
        }
    }

    // ---- write() guards: pretty ----

    @Test
    void write_beforeStartPretty_throwsIllegalStateException() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new JenaModelSerializer(Lang.TURTLE)) {
            var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
            assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
        }
    }

    @Test
    void write_afterEndPretty_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new JenaModelSerializer(Lang.TURTLE)) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();

            var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
            assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
        }
    }

    @Test
    void write_afterClosePretty_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var serializer = new JenaModelSerializer(Lang.TURTLE);
        serializer.start(output, Map.of());
        serializer.write(statement);
        serializer.end();
        serializer.close();

        var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
        assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
    }

    @Test
    void start_calledTwicePretty_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var secondOutput = new ByteArrayOutputStream();
        var emptyMap = Map.<String, String>of();

        try (var serializer = new JenaModelSerializer(Lang.TURTLE)) {
            serializer.start(output, Map.of());

            var thrown = assertThrows(IllegalStateException.class, () -> serializer.start(secondOutput, emptyMap));
            assertThat(thrown.getMessage(), is("start() called while a session is already active"));

            serializer.end();
        }
    }

    // ---- flush ----

    @Test
    void flush_streamingBeforeStart_isNoOp() {
        try (var serializer = new JenaStreamingSerializer(Lang.NTRIPLES)) {
            assertDoesNotThrow(serializer::flush);
        }
    }

    @Test
    void flush_prettyBeforeStart_isNoOp() {
        try (var serializer = new JenaModelSerializer(Lang.TURTLE)) {
            assertDoesNotThrow(serializer::flush);
        }
    }

    // ---- close interactions ----

    @Test
    void close_streamingMultipleCallsIsSafe() {
        var output = new ByteArrayOutputStream();
        var serializer = new JenaStreamingSerializer(Lang.NTRIPLES);

        serializer.start(output, Map.of());
        serializer.end();
        serializer.close();

        assertDoesNotThrow(serializer::close);
    }

    @Test
    void close_prettyMultipleCallsIsSafe() {
        var output = new ByteArrayOutputStream();
        var serializer = new JenaModelSerializer(Lang.TURTLE);

        serializer.start(output, Map.of());
        serializer.end();
        serializer.close();

        assertDoesNotThrow(serializer::close);
    }

    @Test
    void close_streamingDoesNotCloseUnderlyingStream() {
        var closeSpy = new CloseTrackingOutputStream();

        try (var serializer = new JenaStreamingSerializer(Lang.NTRIPLES)) {
            serializer.start(closeSpy, Map.of());
            serializer.write(VF.createStatement(
                    VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing")));
            serializer.end();
        }

        assertThat(closeSpy.wasClosed, is(false));
    }

    @Test
    void close_prettyDoesNotCloseUnderlyingStream() {
        var closeSpy = new CloseTrackingOutputStream();

        try (var serializer = new JenaModelSerializer(Lang.TURTLE)) {
            serializer.start(closeSpy, Map.of());
            serializer.write(VF.createStatement(
                    VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing")));
            serializer.end();
        }

        assertThat(closeSpy.wasClosed, is(false));
    }

    @Test
    void end_streamingAfterClose_isNoOp() {
        var output = new ByteArrayOutputStream();
        var serializer = new JenaStreamingSerializer(Lang.NTRIPLES);

        serializer.start(output, Map.of());
        serializer.end();
        serializer.close();

        var sizeBeforeRedundantEnd = output.size();
        serializer.end();
        assertThat(output.size(), is(sizeBeforeRedundantEnd));
    }

    @Test
    void end_prettyAfterClose_isNoOp() {
        var output = new ByteArrayOutputStream();
        var serializer = new JenaModelSerializer(Lang.TURTLE);

        serializer.start(output, Map.of());
        serializer.end();
        serializer.close();

        var sizeBeforeRedundantEnd = output.size();
        serializer.end();
        assertThat(output.size(), is(sizeBeforeRedundantEnd));
    }

    // ---- byte-level encoding is not supported ----

    @Test
    void supportsByteEncoding_returnsFalse() {
        try (var streaming = new JenaStreamingSerializer(Lang.NTRIPLES);
                var pretty = new JenaModelSerializer(Lang.TURTLE)) {
            assertThat(streaming.supportsByteEncoding(), is(false));
            assertThat(pretty.supportsByteEncoding(), is(false));
        }
    }

    @Test
    void encode_streamingThrowsUnsupportedOperation() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new JenaStreamingSerializer(Lang.NTRIPLES)) {
            assertThrows(UnsupportedOperationException.class, () -> serializer.encode(statement));
        }
    }

    // ---- Jena exceptions are wrapped in RdfSerializationException ----

    @Test
    void end_prettyWrapsJenaException() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var ex = assertThrows(RdfSerializationException.class, () -> runPrettyLifecycle(Lang.TURTLE, statement));
        assertThat(ex.getMessage(), containsString("Turtle"));
    }

    @Test
    void write_streamingWrapsJenaException() {
        // N-Triples writes immediately to the OutputStream on write(), so a FailingOutputStream
        // surfaces the exception during write(), not during end(). Jena's NTriples finish() is a
        // no-op so the end() wrapping path is not exercisable for this format.
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var ex = assertThrows(RdfSerializationException.class, () -> runStreamingLifecycle(Lang.NTRIPLES, statement));
        assertThat(ex.getMessage(), containsString("N-Triples"));
    }

    @Test
    void end_streamingTurtleWrapsJenaException() {
        // Turtle streaming buffers prefix declarations and triple data internally. The
        // OutputStream is written on finish() (called by end()), so a FailAfterNBytesOutputStream
        // that allows initial bytes from write() but fails during finish() exercises the end()
        // wrapping path specifically.
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));
        var namespaces = Map.of("ex", "http://example.org/");

        var ex = assertThrows(
                RdfSerializationException.class,
                () -> runStreamingLifecycleFailingAfter(Lang.TURTLE, statement, namespaces));
        assertThat(ex.getMessage(), containsString("Turtle"));
    }

    private void runStreamingLifecycleFailingAfter(Lang lang, Statement statement, Map<String, String> namespaces) {
        try (var serializer = new JenaStreamingSerializer(lang)) {
            serializer.start(new FailAfterNBytesOutputStream(0), namespaces);
            serializer.write(statement);
            serializer.end();
        }
    }

    // Note: start() exception wrapping for Jena streaming formats cannot be tested via
    // FailingOutputStream because Jena's StreamRDF buffers internally during start()/prefix()
    // and does not write to the underlying OutputStream until the first triple or finish().

    private void runPrettyLifecycle(Lang lang, Statement statement) {
        try (var serializer = new JenaModelSerializer(lang)) {
            serializer.start(new FailingOutputStream(), Map.of());
            serializer.write(statement);
            serializer.end();
        }
    }

    private void runStreamingLifecycle(Lang lang, Statement statement) {
        try (var serializer = new JenaStreamingSerializer(lang)) {
            serializer.start(new FailingOutputStream(), Map.of());
            serializer.write(statement);
            serializer.end();
        }
    }

    /**
     * An OutputStream that tracks whether {@link #close()} was called.
     */
    private static class CloseTrackingOutputStream extends ByteArrayOutputStream {

        boolean wasClosed = false;

        @Override
        public void close() throws IOException {
            wasClosed = true;
            super.close();
        }
    }

    /**
     * An OutputStream that always throws {@link IOException} on any write. Used to force Jena
     * writers to raise exceptions, which the Jena serializers must then wrap in
     * {@link RdfSerializationException}.
     */
    private static class FailingOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            throw new IOException("simulated I/O failure");
        }

        @Override
        public void flush() throws IOException {
            throw new IOException("simulated I/O failure");
        }
    }

    /**
     * An OutputStream that accepts the first {@code limit} bytes, then throws {@link IOException}
     * on every subsequent write. Used to exercise exception wrapping in end()/finish() for formats
     * where Jena buffers internally and only writes to the OutputStream during finish().
     */
    private static class FailAfterNBytesOutputStream extends OutputStream {

        private final int limit;

        private int written;

        FailAfterNBytesOutputStream(int limit) {
            this.limit = limit;
        }

        @Override
        public void write(int b) throws IOException {
            if (written >= limit) {
                throw new IOException("simulated I/O failure after %d bytes".formatted(limit));
            }
            written++;
        }

        @Override
        public void flush() throws IOException {
            if (written >= limit) {
                throw new IOException("simulated I/O failure on flush");
            }
        }
    }
}
