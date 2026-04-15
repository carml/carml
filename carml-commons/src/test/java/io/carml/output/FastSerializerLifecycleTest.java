package io.carml.output;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link RdfSerializer} lifecycle methods on {@link AbstractFastRdfSerializer},
 * exercised through the concrete {@link FastNTriplesSerializer} and {@link FastNQuadsSerializer}.
 */
class FastSerializerLifecycleTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    // ---- start / write / end lifecycle ----

    @Test
    void lifecycle_ntriplesSingleStatement_producesCorrectOutput() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing> .\n"));
    }

    @Test
    void lifecycle_nquadsSingleStatement_producesCorrectOutput() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"),
                RDF.TYPE,
                VF.createIRI("http://example.org/Thing"),
                VF.createIRI("http://example.org/g"));

        try (var serializer = FastNQuadsSerializer.withDefaults()) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing> <http://example.org/g> .\n"));
    }

    @Test
    void lifecycle_nquadsDefaultGraph_omitsGraphField() {
        var output = new ByteArrayOutputStream();
        // Statement with a null context represents the default graph.
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = FastNQuadsSerializer.withDefaults()) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        // No graph IRI between the object and the trailing "."
        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing> .\n"));
    }

    @Test
    void lifecycle_multipleStatements_producesAllLines() {
        var output = new ByteArrayOutputStream();
        var stmt1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));
        var stmt2 = VF.createStatement(VF.createIRI("http://example.org/s2"), RDFS.LABEL, VF.createLiteral("hello"));

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            serializer.start(output, Map.of());
            serializer.write(stmt1);
            serializer.write(stmt2);
            serializer.end();
        }

        var lines = output.toString(StandardCharsets.UTF_8).split("\n");
        assertThat(lines.length, is(2));
        assertThat(lines[0], containsString("http://example.org/s1"));
        assertThat(lines[1], containsString("hello"));
    }

    @Test
    void lifecycle_namespacesIgnored_noPrefix() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(VF.createIRI("http://example.org/s"), RDFS.LABEL, VF.createLiteral("test"));

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            serializer.start(output, Map.of("rdfs", "http://www.w3.org/2000/01/rdf-schema#"));
            serializer.write(statement);
            serializer.end();
        }

        var result = output.toString(StandardCharsets.UTF_8);
        // N-Triples never uses prefix notation — full IRI must be present
        assertThat(result, containsString("<http://www.w3.org/2000/01/rdf-schema#label>"));
    }

    // ---- write() guards ----

    @Test
    void write_beforeStart_throwsIllegalStateException() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
            assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
        }
    }

    @Test
    void write_afterEnd_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.end();

            var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
            assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
        }
    }

    @Test
    void write_afterClose_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var serializer = FastNTriplesSerializer.withDefaults();
        serializer.start(output, Map.of());
        serializer.write(statement);
        serializer.end();
        serializer.close();

        var thrown = assertThrows(IllegalStateException.class, () -> serializer.write(statement));
        assertThat(thrown.getMessage(), is("write() called outside of an active serialization session"));
    }

    @Test
    void start_calledTwice_throwsIllegalStateException() {
        var output = new ByteArrayOutputStream();
        var secondOutput = new ByteArrayOutputStream();
        var emptyMap = Map.<String, String>of();

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            serializer.start(output, Map.of());

            var thrown = assertThrows(IllegalStateException.class, () -> serializer.start(secondOutput, emptyMap));
            assertThat(thrown.getMessage(), is("start() called while a session is already active"));

            serializer.end();
        }
    }

    // ---- flush ----

    @Test
    void flush_afterWrites_outputIsAvailable() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            serializer.start(output, Map.of());
            serializer.write(statement);
            serializer.flush();

            // After flush, the output should contain the statement even before end()
            var result = output.toString(StandardCharsets.UTF_8);
            assertThat(result, containsString("http://example.org/s"));

            serializer.end();
        }
    }

    @Test
    void flush_beforeStart_isNoOp() {
        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            // flush() before start() should not throw
            assertDoesNotThrow(serializer::flush);
        }
    }

    // ---- close / end interactions ----

    @Test
    void close_multipleCallsIsSafe() {
        var output = new ByteArrayOutputStream();
        var serializer = FastNTriplesSerializer.withDefaults();

        serializer.start(output, Map.of());
        serializer.end();
        serializer.close();

        // Second close should not throw
        assertDoesNotThrow(serializer::close);
    }

    @Test
    void close_doesNotCloseUnderlyingStream() {
        var closeSpy = new CloseTrackingOutputStream();

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            serializer.start(closeSpy, Map.of());
            serializer.write(VF.createStatement(
                    VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing")));
            serializer.end();
        }

        assertThat(closeSpy.wasClosed, is(false));
    }

    @Test
    void end_afterClose_isNoOp() {
        var output = new ByteArrayOutputStream();
        var serializer = FastNTriplesSerializer.withDefaults();

        serializer.start(output, Map.of());
        serializer.end();
        serializer.close();
        // end() after close should not throw and should not produce additional output
        var sizeBeforeRedundantEnd = output.size();
        serializer.end();
        assertThat(output.size(), is(sizeBeforeRedundantEnd));
    }

    // ---- byte-level encoding ----

    @Test
    void supportsByteEncoding_returnsTrue() {
        try (var nt = FastNTriplesSerializer.withDefaults();
                var nq = FastNQuadsSerializer.withDefaults()) {
            assertThat(nt.supportsByteEncoding(), is(true));
            assertThat(nq.supportsByteEncoding(), is(true));
        }
    }

    @Test
    void encode_ntriples_producesCorrectBytes() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        byte[] bytes;
        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            bytes = serializer.encode(statement);
        }

        assertThat(bytes, is(notNullValue()));
        var result = new String(bytes, StandardCharsets.UTF_8);
        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing> .\n"));
    }

    @Test
    void encode_nquads_producesCorrectBytes() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"),
                RDF.TYPE,
                VF.createIRI("http://example.org/Thing"),
                VF.createIRI("http://example.org/g"));

        byte[] bytes;
        try (var serializer = FastNQuadsSerializer.withDefaults()) {
            bytes = serializer.encode(statement);
        }

        assertThat(bytes, is(notNullValue()));
        var result = new String(bytes, StandardCharsets.UTF_8);
        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing> <http://example.org/g> .\n"));
    }

    @Test
    void encode_doesNotRequireStart() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        byte[] bytes;
        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            // encode() should work without a prior start() call
            bytes = serializer.encode(statement);
        }
        assertThat(bytes, is(notNullValue()));
        assertThat(new String(bytes, StandardCharsets.UTF_8), containsString("http://example.org/s"));
    }

    // ---- lifecycle and legacy serialize() are independent ----

    @Test
    void lifecycle_andSerializeFlux_areIndependent() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var legacyOutput = new ByteArrayOutputStream();
        var lifecycleOutput = new ByteArrayOutputStream();

        try (var serializer = FastNTriplesSerializer.withDefaults()) {
            // Use the legacy serialize() method
            serializer.serialize(reactor.core.publisher.Flux.just(statement), legacyOutput);

            // Then use the lifecycle API
            serializer.start(lifecycleOutput, Map.of());
            serializer.write(statement);
            serializer.end();
        }

        // Both should produce the same output
        assertThat(lifecycleOutput.toString(StandardCharsets.UTF_8), is(legacyOutput.toString(StandardCharsets.UTF_8)));
    }

    /**
     * An OutputStream that tracks whether {@link #close()} was called.
     */
    private static class CloseTrackingOutputStream extends ByteArrayOutputStream {

        boolean wasClosed = false;

        @Override
        public void close() throws java.io.IOException {
            wasClosed = true;
            super.close();
        }
    }
}
