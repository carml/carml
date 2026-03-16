package io.carml.output;

import static org.eclipse.rdf4j.model.util.Models.isomorphic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;

class FastNQuadsSerializerTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private final FastNQuadsSerializer serializer = FastNQuadsSerializer.withDefaults();

    // --- Statement without graph (default graph, same as N-Triples) ---

    @Test
    void serializeStatement_noGraph_producesNTriplesOutput() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/subject"),
                VF.createIRI("http://example.org/predicate"),
                VF.createIRI("http://example.org/object"));

        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/subject> <http://example.org/predicate> <http://example.org/object> .\n"));
    }

    // --- Statement with IRI graph ---

    @Test
    void serializeStatement_iriGraph_includesGraphField() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/subject"),
                VF.createIRI("http://example.org/predicate"),
                VF.createIRI("http://example.org/object"),
                VF.createIRI("http://example.org/graph"));

        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/subject> <http://example.org/predicate>"
                        + " <http://example.org/object> <http://example.org/graph> .\n"));
    }

    // --- Statement with BNode graph ---

    @Test
    void serializeStatement_bnodeGraph_includesGraphField() {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/subject"),
                VF.createIRI("http://example.org/predicate"),
                VF.createIRI("http://example.org/object"),
                VF.createBNode("g1"));

        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);

        assertThat(
                result,
                is("<http://example.org/subject> <http://example.org/predicate>"
                        + " <http://example.org/object> _:g1 .\n"));
    }

    // --- Parameterized term type cases with graph ---

    static Stream<Arguments> termTypesWithGraphCases() {
        return Stream.of(
                Arguments.of(
                        "IRI subject, IRI object, IRI graph",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"),
                                VF.createIRI("http://example.org/p"),
                                VF.createIRI("http://example.org/o"),
                                VF.createIRI("http://example.org/g")),
                        "<http://example.org/s> <http://example.org/p>"
                                + " <http://example.org/o> <http://example.org/g> .\n"),
                Arguments.of(
                        "BNode subject, IRI graph",
                        VF.createStatement(
                                VF.createBNode("s1"),
                                VF.createIRI("http://example.org/p"),
                                VF.createIRI("http://example.org/o"),
                                VF.createIRI("http://example.org/g")),
                        "_:s1 <http://example.org/p> <http://example.org/o> <http://example.org/g> .\n"),
                Arguments.of(
                        "BNode object, BNode graph",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"),
                                VF.createIRI("http://example.org/p"),
                                VF.createBNode("o1"),
                                VF.createBNode("g1")),
                        "<http://example.org/s> <http://example.org/p> _:o1 _:g1 .\n"),
                Arguments.of(
                        "plain literal object, IRI graph",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"),
                                VF.createIRI("http://example.org/p"),
                                VF.createLiteral("hello"),
                                VF.createIRI("http://example.org/g")),
                        "<http://example.org/s> <http://example.org/p> \"hello\" <http://example.org/g> .\n"),
                Arguments.of(
                        "language-tagged literal, IRI graph",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"),
                                VF.createIRI("http://example.org/p"),
                                VF.createLiteral("Bonjour", "fr"),
                                VF.createIRI("http://example.org/g")),
                        "<http://example.org/s> <http://example.org/p>" + " \"Bonjour\"@fr <http://example.org/g> .\n"),
                Arguments.of(
                        "typed literal, IRI graph",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"),
                                VF.createIRI("http://example.org/p"),
                                VF.createLiteral("42", XSD.INTEGER),
                                VF.createIRI("http://example.org/g")),
                        "<http://example.org/s> <http://example.org/p>"
                                + " \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> <http://example.org/g> .\n"),
                Arguments.of(
                        "xsd:string literal, BNode graph",
                        VF.createStatement(
                                VF.createIRI("http://example.org/s"),
                                VF.createIRI("http://example.org/p"),
                                VF.createLiteral("plain", XSD.STRING),
                                VF.createBNode("g1")),
                        "<http://example.org/s> <http://example.org/p> \"plain\" _:g1 .\n"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("termTypesWithGraphCases")
    void serializeStatement_termTypesWithGraph(String name, Statement statement, String expected) {
        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);
        assertThat(result, is(expected));
    }

    // --- Literal escaping with graph context ---

    static Stream<Arguments> escapeWithGraphCases() {
        return Stream.of(
                Arguments.of("newline in literal", "line1\nline2", "line1\\nline2"),
                Arguments.of("tab in literal", "col1\tcol2", "col1\\tcol2"),
                Arguments.of("double quote in literal", "say \"hi\"", "say \\\"hi\\\""),
                Arguments.of("backslash in literal", "path\\file", "path\\\\file"));
    }

    @ParameterizedTest(name = "escape {0} with graph")
    @MethodSource("escapeWithGraphCases")
    void serializeStatement_literalEscapingWithGraph(
            @SuppressWarnings("unused") String description, String input, String expectedLiteral) {
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"),
                RDFS.LABEL,
                VF.createLiteral(input),
                VF.createIRI("http://example.org/g"));
        var result = new String(serializer.serializeStatement(statement), StandardCharsets.UTF_8);
        assertThat(result, containsString("\"" + expectedLiteral + "\""));
        assertThat(result, containsString("<http://example.org/g>"));
    }

    // --- Multiple statements mixing default and named graphs ---

    @Test
    void serialize_mixedDefaultAndNamedGraphs_producesCorrectOutput() {
        var output = new ByteArrayOutputStream();
        var defaultGraphStmt = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));
        var namedGraphStmt = VF.createStatement(
                VF.createIRI("http://example.org/s2"),
                RDFS.LABEL,
                VF.createLiteral("label"),
                VF.createIRI("http://example.org/graph1"));
        var bnodeGraphStmt = VF.createStatement(
                VF.createIRI("http://example.org/s3"), RDFS.LABEL, VF.createLiteral("other"), VF.createBNode("g1"));

        var count = serializer.serialize(Flux.just(defaultGraphStmt, namedGraphStmt, bnodeGraphStmt), output);

        assertThat(count, is(3L));
        var lines = output.toString(StandardCharsets.UTF_8).split("\n");
        assertThat(lines.length, is(3));
        // Default graph line should not contain a 4th field before the dot
        assertThat(
                lines[0],
                is("<http://example.org/s1>"
                        + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Thing> ."));
        // Named IRI graph line should contain the graph IRI
        assertThat(lines[1], containsString("<http://example.org/graph1>"));
        // BNode graph line should contain the bnode graph
        assertThat(lines[2], containsString("_:g1"));
    }

    // --- Flux-level tests ---

    @Test
    void serialize_emptyFlux_producesEmptyOutput() {
        var output = new ByteArrayOutputStream();

        var count = serializer.serialize(Flux.empty(), output);

        assertThat(count, is(0L));
        assertThat(output.size(), is(0));
    }

    @Test
    void serialize_singleStatementWithGraph_producesOneLine() {
        var output = new ByteArrayOutputStream();
        var statement = VF.createStatement(
                VF.createIRI("http://example.org/s"),
                RDF.TYPE,
                VF.createIRI("http://example.org/Thing"),
                VF.createIRI("http://example.org/g"));

        var count = serializer.serialize(Flux.just(statement), output);

        assertThat(count, is(1L));
        var result = output.toString(StandardCharsets.UTF_8);
        assertThat(
                result,
                is("<http://example.org/s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        + " <http://example.org/Thing> <http://example.org/g> .\n"));
    }

    @Test
    void serialize_largeBatch_processesAllStatements() {
        var output = new ByteArrayOutputStream();
        var statementCount = 10_000;

        Flux<Statement> statements = Flux.fromStream(IntStream.range(0, statementCount)
                .mapToObj(i -> VF.createStatement(
                        VF.createIRI("http://example.org/s%d".formatted(i)),
                        RDF.TYPE,
                        VF.createIRI("http://example.org/Thing"),
                        VF.createIRI("http://example.org/g%d".formatted(i % 10)))));

        var count = serializer.serialize(statements, output);

        assertThat(count, is((long) statementCount));
        var result = output.toString(StandardCharsets.UTF_8);
        var lines = result.split("\n");
        assertThat(lines.length, is(statementCount));
    }

    @Test
    void serialize_batchSizeConfiguration_respected() {
        var smallBatchSerializer = FastNQuadsSerializer.builder().batchSize(2).build();
        var output = new ByteArrayOutputStream();
        var stmt1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"),
                RDF.TYPE,
                VF.createIRI("http://example.org/Thing"),
                VF.createIRI("http://example.org/g"));
        var stmt2 = VF.createStatement(
                VF.createIRI("http://example.org/s2"),
                RDF.TYPE,
                VF.createIRI("http://example.org/Thing"),
                VF.createIRI("http://example.org/g"));
        var stmt3 = VF.createStatement(
                VF.createIRI("http://example.org/s3"), RDF.TYPE, VF.createIRI("http://example.org/Thing"));

        var count = smallBatchSerializer.serialize(Flux.just(stmt1, stmt2, stmt3), output);

        assertThat(count, is(3L));
    }

    // --- Builder validation ---

    @Test
    void builder_invalidBatchSize_throwsException() {
        try {
            FastNQuadsSerializer.builder().batchSize(0);
            assertThat("Expected exception", is(not("Expected exception")));
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), is("Batch size must be positive, got 0"));
        }
    }

    @Test
    void builder_invalidCacheMaxSize_throwsException() {
        try {
            FastNQuadsSerializer.builder().cacheMaxSize(-1);
            assertThat("Expected exception", is(not("Expected exception")));
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), is("Cache max size must be positive, got -1"));
        }
    }

    // --- Round-trip semantic equivalence with Rio NQuadsParser ---

    @Test
    void serializeStatement_outputIsSemanticallyEquivalentToRioNQuadsParser() throws Exception {
        var statements = List.of(
                VF.createStatement(
                        VF.createIRI("http://e.org/s"),
                        RDFS.LABEL,
                        VF.createLiteral("plain"),
                        VF.createIRI("http://e.org/g1")),
                VF.createStatement(VF.createIRI("http://e.org/s"), RDFS.LABEL, VF.createLiteral("no graph")),
                VF.createStatement(
                        VF.createIRI("http://e.org/s"),
                        RDFS.LABEL,
                        VF.createLiteral("tab\there"),
                        VF.createIRI("http://e.org/g2")),
                VF.createStatement(
                        VF.createIRI("http://e.org/s"), RDFS.LABEL, VF.createLiteral("café"), VF.createBNode("g1")),
                VF.createStatement(
                        VF.createIRI("http://e.org/s"),
                        RDFS.LABEL,
                        VF.createLiteral("\uD83D\uDE00"),
                        VF.createIRI("http://e.org/g3")));

        var fastOut = new ByteArrayOutputStream();
        FastNQuadsSerializer.withDefaults().serialize(Flux.fromIterable(statements), fastOut);

        // Parse our output back with Rio's NQuads parser
        var fastModel = Rio.parse(new java.io.ByteArrayInputStream(fastOut.toByteArray()), RDFFormat.NQUADS);

        // Build reference model from the original statements
        var referenceModel = new ModelBuilder().build();
        referenceModel.addAll(statements);

        // Use isomorphic comparison to handle blank node label differences
        assertThat(fastModel.size(), is(statements.size()));
        assertThat(isomorphic(fastModel, referenceModel), is(true));
    }

    // --- Statement order preservation ---

    @Test
    void serialize_multipleFullBatches_preservesStatementOrder() {
        var smallBatchSerializer = FastNQuadsSerializer.builder().batchSize(1).build();
        var output = new ByteArrayOutputStream();
        var stmts = IntStream.range(0, 5)
                .mapToObj(i -> VF.createStatement(
                        VF.createIRI("http://example.org/s" + i),
                        RDF.TYPE,
                        VF.createIRI("http://example.org/T"),
                        VF.createIRI("http://example.org/g" + i)))
                .toList();

        smallBatchSerializer.serialize(Flux.fromIterable(stmts), output);

        var lines = output.toString(StandardCharsets.UTF_8).split("\n");
        assertThat(lines.length, is(5));
        for (int i = 0; i < 5; i++) {
            assertThat(lines[i], containsString("http://example.org/s" + i));
            assertThat(lines[i], containsString("http://example.org/g" + i));
        }
    }

    // --- IRI cache consistency with graph terms ---

    @Test
    void serialize_sharedGraphIri_cacheProducesConsistentOutput() {
        var output = new ByteArrayOutputStream();
        var sharedGraph = VF.createIRI("http://example.org/shared-graph");
        var stmt1 = VF.createStatement(
                VF.createIRI("http://example.org/s1"), RDF.TYPE, VF.createIRI("http://example.org/T"), sharedGraph);
        var stmt2 = VF.createStatement(
                VF.createIRI("http://example.org/s2"), RDF.TYPE, VF.createIRI("http://example.org/T"), sharedGraph);

        var count = serializer.serialize(Flux.just(stmt1, stmt2), output);

        assertThat(count, is(2L));
        var result = output.toString(StandardCharsets.UTF_8);
        var occurrences = result.split("<http://example.org/shared-graph>").length - 1;
        assertThat(occurrences, is(2));
    }
}
