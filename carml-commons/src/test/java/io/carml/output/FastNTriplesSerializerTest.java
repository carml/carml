package io.carml.output;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.ntriples.NTriplesWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;

class FastNTriplesSerializerTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private final FastNTriplesSerializer serializer = FastNTriplesSerializer.withDefaults();

    @Test
    void serializeStatement_basicIriTriple_producesCorrectNTriple() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/subject"),
                VF.createIRI("http://example.org/predicate"),
                VF.createIRI("http://example.org/object"));

        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/subject> <http://example.org/predicate> <http://example.org/object> .\n"));
    }

    static Stream<Arguments> termTypeCases() {
        return Stream.of(
                Arguments.of(
                        "plain string literal",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"), RDFS.LABEL, VF.createLiteral("Hello World")),
                        "<http://example.org/s> <http://www.w3.org/2000/01/rdf-schema#label> \"Hello World\" .\n"),
                Arguments.of(
                        "literal with language tag",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"), RDFS.LABEL, VF.createLiteral("Bonjour", "fr")),
                        "<http://example.org/s> <http://www.w3.org/2000/01/rdf-schema#label> \"Bonjour\"@fr .\n"),
                Arguments.of(
                        "literal with datatype",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"),
                                VF.createIRI("http://example.org/age"),
                                VF.createLiteral("42", XSD.INTEGER)),
                        "<http://example.org/s> <http://example.org/age> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n"),
                Arguments.of(
                        "blank node subject",
                        VF.createStatement(VF.createBNode("node1"), RDF.TYPE, VF.createIRI("http://example.org/Thing")),
                        "_:node1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Thing> .\n"),
                Arguments.of(
                        "blank node object",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"),
                                VF.createIRI("http://example.org/rel"),
                                VF.createBNode("node2")),
                        "<http://example.org/s> <http://example.org/rel> _:node2 .\n"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("termTypeCases")
    void serializeStatement_termTypes(String name, Statement statement, String expected) {
        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);
        assertThat(result, is(expected));
    }

    static Stream<Arguments> escapeCases() {
        return Stream.of(
                Arguments.of("newline", "line1\nline2", "line1\\nline2"),
                Arguments.of("tab", "col1\tcol2", "col1\\tcol2"),
                Arguments.of("carriage return", "line1\rline2", "line1\\rline2"),
                Arguments.of("double quote", "say \"hello\"", "say \\\"hello\\\""),
                Arguments.of("backslash", "path\\to\\file", "path\\\\to\\\\file"));
    }

    @ParameterizedTest(name = "escape {0}")
    @MethodSource("escapeCases")
    void serializeStatement_literalEscaping(String description, String input, String expectedLiteral) {
        var statement = VF.createStatement(VF.createIRI("http://example.org/s"), RDFS.LABEL, VF.createLiteral(input));
        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);
        var expected = "<http://example.org/s> <http://www.w3.org/2000/01/rdf-schema#label> \"" + expectedLiteral
                + "\" ." + "\n";
        assertThat(result, is(expected));
    }

    static Stream<Arguments> unicodeEscapeCases() {
        return Stream.of(
                Arguments.of("non-ASCII BMP (U+00E9)", "café", "caf\\u00E9"),
                Arguments.of("supplementary (U+1F600)", "smile\uD83D\uDE00end", "smile\\U0001F600end"),
                Arguments.of("control char (U+0001)", "before\u0001after", "before\\u0001after"));
    }

    @ParameterizedTest(name = "unicode escape {0}")
    @MethodSource("unicodeEscapeCases")
    void serializeStatement_unicodeEscaping(String description, String input, String expectedLiteral) {
        var statement = VF.createStatement(VF.createIRI("http://example.org/s"), RDFS.LABEL, VF.createLiteral(input));
        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);
        assertThat(result, containsString("\"" + expectedLiteral + "\""));
    }

    @Test
    void serialize_emptyFlux_producesEmptyOutput() {
        var output = new ByteArrayOutputStream();

        var count = serializer.serialize(Flux.empty(), output);

        assertThat(count, is(0L));
        assertThat(output.size(), is(0));
    }

    @Test
    void serialize_singleStatement_producesOneLine() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var count = serializer.serialize(Flux.just(statement), output);

        assertThat(count, is(1L));
        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing> .\n"));
    }

    @Test
    void serialize_multipleStatements_producesMultipleLines() {
        var output = new ByteArrayOutputStream();
        var stmt1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));
        var stmt2 = VF.createStatement(VF.createIRI("http://example.org/s2"), RDFS.LABEL, VF.createLiteral("label"));

        var count = serializer.serialize(Flux.just(stmt1, stmt2), output);

        assertThat(count, is(2L));
        var lines = output.toString(StandardCharsets.UTF_8).split("\n");
        assertThat(lines.length, is(2));
    }

    @Test
    void serialize_iriCacheHit_producesConsistentOutput() {
        var output = new ByteArrayOutputStream();
        var sharedPredicate = VF.createIRI("http://example.org/shared-predicate");
        var stmt1 = VF.createStatement(VF.createIRI("http://example.org/s1"), sharedPredicate, VF.createLiteral("a"));
        var stmt2 = VF.createStatement(VF.createIRI("http://example.org/s2"), sharedPredicate, VF.createLiteral("b"));

        var count = serializer.serialize(Flux.just(stmt1, stmt2), output);

        assertThat(count, is(2L));
        var result = output.toString(StandardCharsets.UTF_8);
        // Both lines must contain the same predicate serialization
        assertThat(result.contains("<http://example.org/shared-predicate>"), is(true));
        var occurrences = result.split("<http://example.org/shared-predicate>").length - 1;
        assertThat(occurrences, is(2));
    }

    @Test
    void serialize_largeBatch_processesAllStatements() {
        var output = new ByteArrayOutputStream();
        var statementCount = 10_000;

        Flux<Statement> statements = Flux.fromStream(IntStream.range(0, statementCount)
                .mapToObj(i -> VF.createStatement(
                        VF.createIRI("http://example.org/s%d".formatted(i)),
                        RDF.TYPE,
                        VF.createIRI("http://example.org/Thing"))));

        var count = serializer.serialize(statements, output);

        assertThat(count, is((long) statementCount));
        var result = output.toString(StandardCharsets.UTF_8);
        var lines = result.split("\n");
        assertThat(lines.length, is(statementCount));
    }

    @Test
    void serialize_batchSizeConfiguration_respected() {
        var smallBatchSerializer = FastNTriplesSerializer.builder().batchSize(2).build();
        var output = new ByteArrayOutputStream();
        var stmt1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));
        var stmt2 = VF.createStatement(
                VF.createIRI("http://example.org/s2"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));
        var stmt3 = VF.createStatement(
                VF.createIRI("http://example.org/s3"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var count = smallBatchSerializer.serialize(Flux.just(stmt1, stmt2, stmt3), output);

        assertThat(count, is(3L));
    }

    @Test
    void builder_invalidBatchSize_throwsException() {
        try {
            FastNTriplesSerializer.builder().batchSize(0);
            assertThat("Expected exception", is(not("Expected exception")));
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), is("Batch size must be positive, got 0"));
        }
    }

    @Test
    void builder_invalidCacheMaxSize_throwsException() {
        try {
            FastNTriplesSerializer.builder().cacheMaxSize(-1);
            assertThat("Expected exception", is(not("Expected exception")));
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), is("Cache max size must be positive, got -1"));
        }
    }

    @Test
    void lruTermCache_evictsOldEntries_whenMaxSizeExceeded() {
        var smallCacheSerializer =
                FastNTriplesSerializer.builder().cacheMaxSize(2).build();

        var iri1 = VF.createIRI("http://example.org/first");
        var iri2 = VF.createIRI("http://example.org/second");
        var iri3 = VF.createIRI("http://example.org/third");

        var callCount = new AtomicInteger();
        Function<Value, byte[]> countingEncoder = v -> {
            callCount.incrementAndGet();
            return FastNTriplesSerializer.encodeIri(v);
        };

        smallCacheSerializer.getOrComputeCached(iri1, countingEncoder); // miss
        smallCacheSerializer.getOrComputeCached(iri2, countingEncoder); // miss
        smallCacheSerializer.getOrComputeCached(iri3, countingEncoder); // miss, evicts iri1
        assertThat(callCount.get(), is(3));

        smallCacheSerializer.getOrComputeCached(iri1, countingEncoder); // miss (iri1 was evicted)
        assertThat(callCount.get(), is(4));

        // Note: computeIfAbsent on LinkedHashMap does not update access order on cache hits,
        // so iri2 was evicted when iri1 was re-inserted (iri2 was the eldest non-accessed entry).
        // iri3 should still be cached since it was inserted most recently.
        smallCacheSerializer.getOrComputeCached(iri3, countingEncoder); // hit (still cached)
        assertThat(callCount.get(), is(4)); // unchanged
    }

    @Test
    void serializeStatement_rdfTypePredicate_producesCorrectOutput() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"), RDF.TYPE, VF.createIRI("http://xmlns.com/foaf/0.1/Person"));

        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://xmlns.com/foaf/0.1/Person> .\n"));
    }

    @Test
    void serializeStatement_literalWithMultipleEscapes_escapesAll() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"),
                RDFS.LABEL,
                VF.createLiteral("tab\there\nnewline\r\"quoted\"\\backslash"));

        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/2000/01/rdf-schema#label>"
                        + " \"tab\\there\\nnewline\\r\\\"quoted\\\"\\\\backslash\" .\n"));
    }

    @Test
    void serializeStatement_booleanLiteral_includesDatatype() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"),
                VF.createIRI("http://example.org/active"),
                VF.createLiteral("true", XSD.BOOLEAN));

        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/s> <http://example.org/active>"
                        + " \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n"));
    }

    @Test
    void serializeStatement_outputIsSemanticallyEquivalentToRioNTriplesWriter() throws Exception {
        var statements = List.of(
                VF.createStatement(VF.createIRI("http://e.org/s"), RDFS.LABEL, VF.createLiteral("plain")),
                VF.createStatement(VF.createIRI("http://e.org/s"), RDFS.LABEL, VF.createLiteral("tab\there")),
                VF.createStatement(VF.createIRI("http://e.org/s"), RDFS.LABEL, VF.createLiteral("\u007F")),
                VF.createStatement(VF.createIRI("http://e.org/s"), RDFS.LABEL, VF.createLiteral("café")),
                VF.createStatement(VF.createIRI("http://e.org/s"), RDFS.LABEL, VF.createLiteral("\uD83D\uDE00")));

        var rioOut = new ByteArrayOutputStream();
        var rioWriter = new NTriplesWriter(rioOut);
        rioWriter.startRDF();
        statements.forEach(rioWriter::handleStatement);
        rioWriter.endRDF();

        var fastOut = new ByteArrayOutputStream();
        FastNTriplesSerializer.withDefaults().serialize(Flux.fromIterable(statements), fastOut);

        // Rio writes non-ASCII as raw UTF-8, FastNTriplesSerializer uses unicode escapes.
        // Both are valid N-Triples. Parse both outputs back into models and compare semantically.
        var rioModel = Rio.parse(new java.io.ByteArrayInputStream(rioOut.toByteArray()), RDFFormat.NTRIPLES);
        var fastModel = Rio.parse(new java.io.ByteArrayInputStream(fastOut.toByteArray()), RDFFormat.NTRIPLES);

        assertThat(fastModel, is(rioModel));
    }

    @Test
    void writeEscapedLiteralBytes_delControlChar_escapesWithUnicodeEscape() {
        var stmt = VF.createStatement(
                VF.createIRI("http://example.org/s"),
                VF.createIRI("http://example.org/p"),
                VF.createLiteral("before\u007Fafter"));

        var result = new String(serializer.serializeStatement(stmt), StandardCharsets.UTF_8);

        assertThat(result, containsString("before\\u007Fafter"));
    }

    @Test
    void serialize_multipleFullBatches_preservesStatementOrder() {
        var smallBatchSerializer = FastNTriplesSerializer.builder().batchSize(1).build();
        var output = new ByteArrayOutputStream();
        var stmts = IntStream.range(0, 5)
                .mapToObj(i -> VF.createStatement(
                        VF.createIRI("http://example.org/s" + i), RDF.TYPE, VF.createIRI("http://example.org/T")))
                .toList();

        smallBatchSerializer.serialize(Flux.fromIterable(stmts), output);

        var lines = output.toString(StandardCharsets.UTF_8).split("\n");
        assertThat(lines.length, is(5));
        for (int i = 0; i < 5; i++) {
            assertThat(lines[i], containsString("http://example.org/s" + i));
        }
    }
}
