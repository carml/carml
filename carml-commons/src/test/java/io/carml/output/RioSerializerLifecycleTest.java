package io.carml.output;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link RdfSerializer} lifecycle contract for the Rio-backed serializers
 * ({@link RioStreamingSerializer} and {@link RioModelSerializer}). Mirrors
 * {@link FastSerializerLifecycleTest}'s structure to ensure both SPI families behave
 * consistently.
 */
class RioSerializerLifecycleTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    // ---- streaming: start / write / end produces parseable output ----

    @Test
    void lifecycle_streamingTurtleSingleStatement_producesTurtleOutput() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new RioStreamingSerializer(RDFFormat.TURTLE)) {
            serializer.start(output, Map.of("ex", "http://example.org/"));
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        // With the ex: prefix registered, Turtle output shortens IRIs.
        assertThat(result, containsString("@prefix ex:"));
        assertThat(result, containsString("ex:s"));
        assertThat(result, containsString("ex:Thing"));
    }

    @Test
    void lifecycle_streamingNTriples_producesNTriplesOutput() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES)) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(
                result,
                containsString("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing> ."));
    }

    // ---- pretty: buffers until end, then emits complete document ----

    @Test
    void lifecycle_prettyTurtle_emitsOnEnd() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new RioModelSerializer(RDFFormat.TURTLE)) {
            serializer.start(output, Map.of("ex", "http://example.org/"));
            serializer.write(statement);
            // Before end() the output stream should still be empty — pretty mode buffers.
            assertThat(output.size(), is(0));
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        // Prefix was registered, so pretty output uses it (ex:s rather than the full IRI).
        assertThat(result, containsString("@prefix ex:"));
        assertThat(result, containsString("ex:s"));
    }

    @Test
    void lifecycle_prettyTurtleMultipleStatements_emitsAll() {
        var output = new ByteArrayOutputStream();
        var stmt1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));
        var stmt2 = VF.createStatement(VF.createIRI("http://example.org/s2"), RDFS.LABEL, VF.createLiteral("hello"));

        try (var serializer = new RioModelSerializer(RDFFormat.TURTLE)) {
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

        try (var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES)) {
            var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
            assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
        }
    }

    @Test
    void write_afterEndStreaming_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES)) {
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

        var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES);
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

        try (var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES)) {
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

        try (var serializer = new RioModelSerializer(RDFFormat.TURTLE)) {
            var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
            assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
        }
    }

    @Test
    void write_afterEndPretty_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new RioModelSerializer(RDFFormat.TURTLE)) {
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

        var serializer = new RioModelSerializer(RDFFormat.TURTLE);
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

        try (var serializer = new RioModelSerializer(RDFFormat.TURTLE)) {
            serializer.start(output, Map.of());

            var thrown = assertThrows(IllegalStateException.class, () -> serializer.start(secondOutput, emptyMap));
            assertThat(thrown.getMessage(), is("start() called while a session is already active"));

            serializer.end();
        }
    }

    // ---- flush ----

    @Test
    void flush_streamingBeforeStart_isNoOp() {
        try (var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES)) {
            assertDoesNotThrow(serializer::flush);
        }
    }

    @Test
    void flush_prettyBeforeStart_isNoOp() {
        try (var serializer = new RioModelSerializer(RDFFormat.TURTLE)) {
            assertDoesNotThrow(serializer::flush);
        }
    }

    // ---- close interactions ----

    @Test
    void close_streamingMultipleCallsIsSafe() {
        var output = new ByteArrayOutputStream();
        var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES);

        serializer.start(output, Map.of());
        serializer.end();
        serializer.close();

        assertDoesNotThrow(serializer::close);
    }

    @Test
    void close_prettyMultipleCallsIsSafe() {
        var output = new ByteArrayOutputStream();
        var serializer = new RioModelSerializer(RDFFormat.TURTLE);

        serializer.start(output, Map.of());
        serializer.end();
        serializer.close();

        assertDoesNotThrow(serializer::close);
    }

    @Test
    void close_streamingDoesNotCloseUnderlyingStream() {
        var closeSpy = new CloseTrackingOutputStream();

        try (var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES)) {
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

        try (var serializer = new RioModelSerializer(RDFFormat.TURTLE)) {
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
        var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES);

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
        var serializer = new RioModelSerializer(RDFFormat.TURTLE);

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
        try (var streaming = new RioStreamingSerializer(RDFFormat.NTRIPLES);
                var pretty = new RioModelSerializer(RDFFormat.TURTLE)) {
            assertThat(streaming.supportsByteEncoding(), is(false));
            assertThat(pretty.supportsByteEncoding(), is(false));
        }
    }

    @Test
    void encode_streamingThrowsUnsupportedOperation() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = new RioStreamingSerializer(RDFFormat.NTRIPLES)) {
            assertThrows(UnsupportedOperationException.class, () -> serializer.encode(statement));
        }
    }

    // ---- Rio handler exceptions are wrapped in RdfSerializationException (W1 fix) ----

    @Test
    void end_prettyWrapsRdfHandlerException() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var ex = assertThrows(RdfSerializationException.class, () -> runPrettyLifecycle(RDFFormat.TURTLE, statement));
        assertThat(ex.getMessage(), containsString("Turtle"));
    }

    @Test
    void end_streamingWrapsRdfHandlerException() {
        // Rio's N-Triples writer buffers the line-encoded output in an internal java.io.Writer;
        // the underlying OutputStream is only touched on endRDF()'s flush. A FailingOutputStream
        // therefore surfaces an RDFHandlerException during end(), which the streaming serializer
        // must wrap in RdfSerializationException.
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var ex = assertThrows(
                RdfSerializationException.class, () -> runStreamingLifecycle(RDFFormat.NTRIPLES, statement));
        assertThat(ex.getMessage(), containsString("N-Triples"));
    }

    private void runPrettyLifecycle(RDFFormat format, Statement statement) {
        try (var serializer = new RioModelSerializer(format)) {
            serializer.start(new FailingOutputStream(), Map.of());
            serializer.write(statement);
            serializer.end();
        }
    }

    private void runStreamingLifecycle(RDFFormat format, Statement statement) {
        try (var serializer = new RioStreamingSerializer(format)) {
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
     * An OutputStream that always throws {@link IOException} on any write. Used to force Rio
     * writers to raise {@code RDFHandlerException}, which the Rio serializers must then wrap in
     * {@link RdfSerializationException}. Only {@link #write(int)} and {@link #flush()} are
     * overridden; the inherited {@code write(byte[], int, int)} loops over {@code write(int)} so
     * every byte still goes through the failing path.
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
}
