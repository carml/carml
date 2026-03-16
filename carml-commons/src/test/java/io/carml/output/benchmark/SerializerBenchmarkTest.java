package io.carml.output.benchmark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import io.carml.output.FastNQuadsSerializer;
import io.carml.output.FastNTriplesSerializer;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.Quad;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.Rio;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;

/**
 * Performance benchmark comparing fast NT/NQ serializers against Rio and Jena writers.
 *
 * <p>Measures wall time (ms), throughput (statements/second), and memory delta (MB) for each
 * serializer on 100K and 1M statement workloads.
 *
 * <p>Serializers benchmarked for each format (N-Triples and N-Quads):
 * <ul>
 *   <li><strong>Fast (Flux)</strong>: {@link FastNTriplesSerializer} / {@link FastNQuadsSerializer}
 *       via the {@code serialize(Flux, OutputStream)} API -- the production code path that collects
 *       statements into batches via Flux</li>
 *   <li><strong>Fast (direct)</strong>: Same serializer using per-statement
 *       {@code serializeStatement()} -- isolates raw serialization speed from Flux overhead</li>
 *   <li><strong>Rio</strong>: RDF4J Rio's {@code NTriplesWriter} / {@code NQuadsWriter} via
 *       per-statement {@code handleStatement()}</li>
 *   <li><strong>Jena</strong>: Apache Jena's {@code StreamRDFWriter} with RDF4J-to-Jena
 *       conversion via per-statement {@code triple()} / {@code quad()}</li>
 * </ul>
 *
 * <p>Test data includes a realistic mix of IRI subjects, blank node subjects, multiple shared
 * predicates (to exercise IRI cache effectiveness), and literals: plain, language-tagged, and typed
 * (integer, boolean). For N-Quads: a mix of default graph and named graphs.
 *
 * <p>This class is placed in the {@code benchmark} package which is excluded from normal test runs
 * via surefire's {@code <excludes>} configuration. Run explicitly with the {@code benchmark} Maven
 * profile:
 *
 * <pre>{@code
 * mvn test -pl carml-commons -Dcheckstyle.skip=true -Pbenchmark
 * }</pre>
 *
 * <p><strong>Warm-up:</strong> A warm-up pass of 10K statements is run for every serializer before
 * measured scenarios to trigger JIT compilation and internal cache warm-up.
 *
 * <p><strong>Note:</strong> Results vary with hardware, JVM version, and background load. The
 * benchmark is designed for relative comparison on the same machine, not absolute throughput.
 */
@Slf4j
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
class SerializerBenchmarkTest {

    private static final int WARM_UP_SIZE = 10_000;
    private static final int[] STATEMENT_COUNTS = {100_000, 1_000_000};

    /** Shared predicates to exercise IRI cache effectiveness. */
    private static final String[] PREDICATES = {
        "http://schema.org/name",
        "http://schema.org/description",
        "http://schema.org/dateCreated",
        "http://schema.org/identifier",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://xmlns.com/foaf/0.1/knows",
        "http://purl.org/dc/terms/title",
        "http://www.w3.org/2000/01/rdf-schema#label",
    };

    /** Named graphs for N-Quads benchmarks. */
    private static final String[] GRAPHS = {
        null, // default graph
        "http://example.org/graph/1",
        "http://example.org/graph/2",
        "http://example.org/graph/3",
    };

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private List<Statement> ntriplesStatements100k;
    private List<Statement> ntriplesStatements1m;
    private List<Statement> nquadsStatements100k;
    private List<Statement> nquadsStatements1m;

    @BeforeAll
    void setUp() {
        LOG.info("Generating synthetic test data...");
        ntriplesStatements100k = generateStatements(100_000, false);
        ntriplesStatements1m = generateStatements(1_000_000, false);
        nquadsStatements100k = generateStatements(100_000, true);
        nquadsStatements1m = generateStatements(1_000_000, true);
        LOG.info("Test data generation complete.");
    }

    // --- N-Triples benchmarks ---

    @Test
    void benchmark_ntriples_allScalesAllSerializers() {
        LOG.info("");
        LOG.info("=== N-Triples Serializer Benchmark ===");
        LOG.info("{}", formatHeader());
        LOG.info("{}", formatSeparator());

        // Warm up all serializers
        var warmUp = ntriplesStatements100k.subList(0, WARM_UP_SIZE);
        runFastNTriplesFlux(warmUp);
        runFastNTriplesDirect(warmUp);
        runRioNTriples(warmUp);
        runJenaNTriples(warmUp);

        for (int count : STATEMENT_COUNTS) {
            var statements = count == 100_000 ? ntriplesStatements100k : ntriplesStatements1m;

            var fastFluxResult = runFastNTriplesFlux(statements);
            var fastDirectResult = runFastNTriplesDirect(statements);
            var rioResult = runRioNTriples(statements);
            var jenaResult = runJenaNTriples(statements);

            LOG.info("{}", formatRow("N-Triples", count, "Fast (Flux)", fastFluxResult));
            LOG.info("{}", formatRow("N-Triples", count, "Fast (direct)", fastDirectResult));
            LOG.info("{}", formatRow("N-Triples", count, "Rio", rioResult));
            LOG.info("{}", formatRow("N-Triples", count, "Jena (+convert)", jenaResult));
            LOG.info("{}", formatSpeedupRow("N-Triples", count, "Flux vs Rio", fastFluxResult, rioResult));
            LOG.info("{}", formatSpeedupRow("N-Triples", count, "Direct vs Rio", fastDirectResult, rioResult));
            LOG.info("{}", formatSpeedupRow("N-Triples", count, "Direct vs Jena", fastDirectResult, jenaResult));
            LOG.info("{}", formatSeparator());

            assertThat(
                    "Fast serializer should serialize %d statements".formatted(count),
                    fastFluxResult.statementCount(),
                    greaterThan(0L));
        }
    }

    // --- N-Quads benchmarks ---

    @Test
    void benchmark_nquads_allScalesAllSerializers() {
        LOG.info("");
        LOG.info("=== N-Quads Serializer Benchmark ===");
        LOG.info("{}", formatHeader());
        LOG.info("{}", formatSeparator());

        // Warm up all serializers
        var warmUp = nquadsStatements100k.subList(0, WARM_UP_SIZE);
        runFastNQuadsFlux(warmUp);
        runFastNQuadsDirect(warmUp);
        runRioNQuads(warmUp);
        runJenaNQuads(warmUp);

        for (int count : STATEMENT_COUNTS) {
            var statements = count == 100_000 ? nquadsStatements100k : nquadsStatements1m;

            var fastFluxResult = runFastNQuadsFlux(statements);
            var fastDirectResult = runFastNQuadsDirect(statements);
            var rioResult = runRioNQuads(statements);
            var jenaResult = runJenaNQuads(statements);

            LOG.info("{}", formatRow("N-Quads", count, "Fast (Flux)", fastFluxResult));
            LOG.info("{}", formatRow("N-Quads", count, "Fast (direct)", fastDirectResult));
            LOG.info("{}", formatRow("N-Quads", count, "Rio", rioResult));
            LOG.info("{}", formatRow("N-Quads", count, "Jena (+convert)", jenaResult));
            LOG.info("{}", formatSpeedupRow("N-Quads", count, "Flux vs Rio", fastFluxResult, rioResult));
            LOG.info("{}", formatSpeedupRow("N-Quads", count, "Direct vs Rio", fastDirectResult, rioResult));
            LOG.info("{}", formatSpeedupRow("N-Quads", count, "Direct vs Jena", fastDirectResult, jenaResult));
            LOG.info("{}", formatSeparator());

            assertThat(
                    "Fast serializer should serialize %d statements".formatted(count),
                    fastFluxResult.statementCount(),
                    greaterThan(0L));
        }
    }

    // --- Fast serializer runners (Flux API -- production code path) ---

    private BenchmarkResult runFastNTriplesFlux(List<Statement> statements) {
        var serializer = FastNTriplesSerializer.withDefaults();
        var output = NullOutputStream.INSTANCE;

        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        long count = serializer.serialize(Flux.fromIterable(statements), output);
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();

        return new BenchmarkResult(count, (endNanos - startNanos) / 1_000_000, Math.max(0, memAfter - memBefore));
    }

    private BenchmarkResult runFastNQuadsFlux(List<Statement> statements) {
        var serializer = FastNQuadsSerializer.withDefaults();
        var output = NullOutputStream.INSTANCE;

        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        long count = serializer.serialize(Flux.fromIterable(statements), output);
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();

        return new BenchmarkResult(count, (endNanos - startNanos) / 1_000_000, Math.max(0, memAfter - memBefore));
    }

    // --- Fast serializer runners (direct per-statement API -- isolates serialization speed) ---

    private BenchmarkResult runFastNTriplesDirect(List<Statement> statements) {
        var serializer = FastNTriplesSerializer.withDefaults();
        var output = CountingNullOutputStream.create();

        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        for (var statement : statements) {
            output.write(serializer.serializeStatement(statement));
        }
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();

        return new BenchmarkResult(
                statements.size(), (endNanos - startNanos) / 1_000_000, Math.max(0, memAfter - memBefore));
    }

    private BenchmarkResult runFastNQuadsDirect(List<Statement> statements) {
        var serializer = FastNQuadsSerializer.withDefaults();
        var output = CountingNullOutputStream.create();

        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        for (var statement : statements) {
            output.write(serializer.serializeStatement(statement));
        }
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();

        return new BenchmarkResult(
                statements.size(), (endNanos - startNanos) / 1_000_000, Math.max(0, memAfter - memBefore));
    }

    // --- Rio serializer runners ---

    private BenchmarkResult runRioNTriples(List<Statement> statements) {
        var output = NullOutputStream.INSTANCE;

        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        var writer = Rio.createWriter(org.eclipse.rdf4j.rio.RDFFormat.NTRIPLES, output);
        writer.startRDF();
        for (var statement : statements) {
            writer.handleStatement(statement);
        }
        writer.endRDF();
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();

        return new BenchmarkResult(
                statements.size(), (endNanos - startNanos) / 1_000_000, Math.max(0, memAfter - memBefore));
    }

    private BenchmarkResult runRioNQuads(List<Statement> statements) {
        var output = NullOutputStream.INSTANCE;

        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        var writer = Rio.createWriter(org.eclipse.rdf4j.rio.RDFFormat.NQUADS, output);
        writer.startRDF();
        for (var statement : statements) {
            writer.handleStatement(statement);
        }
        writer.endRDF();
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();

        return new BenchmarkResult(
                statements.size(), (endNanos - startNanos) / 1_000_000, Math.max(0, memAfter - memBefore));
    }

    // --- Jena serializer runners (includes RDF4J-to-Jena conversion overhead) ---

    private BenchmarkResult runJenaNTriples(List<Statement> statements) {
        var output = NullOutputStream.INSTANCE;

        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        StreamRDF stream = StreamRDFWriter.getWriterStream(output, RDFFormat.NTRIPLES_UTF8);
        stream.start();
        for (var statement : statements) {
            stream.triple(toJenaTriple(statement));
        }
        stream.finish();
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();

        return new BenchmarkResult(
                statements.size(), (endNanos - startNanos) / 1_000_000, Math.max(0, memAfter - memBefore));
    }

    private BenchmarkResult runJenaNQuads(List<Statement> statements) {
        var output = NullOutputStream.INSTANCE;

        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        StreamRDF stream = StreamRDFWriter.getWriterStream(output, RDFFormat.NQUADS_UTF8);
        stream.start();
        for (var statement : statements) {
            stream.quad(toJenaQuad(statement));
        }
        stream.finish();
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();

        return new BenchmarkResult(
                statements.size(), (endNanos - startNanos) / 1_000_000, Math.max(0, memAfter - memBefore));
    }

    // --- RDF4J-to-Jena conversion (inlined to avoid carml-converters-jena dependency) ---

    private static org.apache.jena.graph.Triple toJenaTriple(Statement statement) {
        return org.apache.jena.graph.Triple.create(
                toJenaNode(statement.getSubject()),
                toJenaNode(statement.getPredicate()),
                toJenaNode(statement.getObject()));
    }

    private static Quad toJenaQuad(Statement statement) {
        var context = statement.getContext();
        var graphNode = context == null ? Quad.defaultGraphIRI : toJenaNode(context);
        return Quad.create(
                graphNode,
                toJenaNode(statement.getSubject()),
                toJenaNode(statement.getPredicate()),
                toJenaNode(statement.getObject()));
    }

    private static org.apache.jena.graph.Node toJenaNode(org.eclipse.rdf4j.model.Value value) {
        if (value instanceof IRI iri) {
            return org.apache.jena.graph.NodeFactory.createURI(iri.stringValue());
        } else if (value instanceof BNode bNode) {
            return org.apache.jena.graph.NodeFactory.createBlankNode(bNode.getID());
        } else if (value instanceof Literal literal) {
            return literal.getLanguage()
                    .map(lang -> org.apache.jena.graph.NodeFactory.createLiteralLang(literal.stringValue(), lang))
                    .orElseGet(() -> org.apache.jena.graph.NodeFactory.createLiteralDT(
                            literal.stringValue(),
                            org.apache.jena.datatypes.TypeMapper.getInstance()
                                    .getSafeTypeByName(literal.getDatatype().stringValue())));
        }
        throw new IllegalArgumentException("Unsupported value type: %s".formatted(value.getClass()));
    }

    // --- Test data generation ---

    /**
     * Generates synthetic RDF4J statements with a realistic mix of term types:
     * <ul>
     *   <li>~80% IRI subjects, ~20% blank node subjects</li>
     *   <li>Shared predicates from a fixed pool (exercises IRI cache)</li>
     *   <li>Object mix: ~40% IRI, ~20% plain literal, ~15% language-tagged, ~15% typed integer,
     *       ~10% typed boolean</li>
     *   <li>For NQ: ~25% default graph, ~75% named graphs from a fixed pool</li>
     * </ul>
     */
    private static List<Statement> generateStatements(int count, boolean withGraphs) {
        var random = new Random(42); // Fixed seed for reproducibility
        var statements = new ArrayList<Statement>(count);

        for (int i = 0; i < count; i++) {
            var subject = generateSubject(i, random);
            var predicate = VF.createIRI(PREDICATES[random.nextInt(PREDICATES.length)]);
            var object = generateObject(i, random);

            if (withGraphs) {
                var graphName = GRAPHS[random.nextInt(GRAPHS.length)];
                var context = graphName != null ? VF.createIRI(graphName) : null;
                statements.add(VF.createStatement(subject, predicate, object, context));
            } else {
                statements.add(VF.createStatement(subject, predicate, object));
            }
        }

        return statements;
    }

    private static org.eclipse.rdf4j.model.Resource generateSubject(int index, Random random) {
        if (random.nextDouble() < 0.2) {
            return VF.createBNode("node%d".formatted(index));
        }
        return VF.createIRI("http://example.org/resource/%d".formatted(index));
    }

    private static org.eclipse.rdf4j.model.Value generateObject(int index, Random random) {
        double roll = random.nextDouble();
        if (roll < 0.40) {
            // IRI object
            return VF.createIRI("http://example.org/target/%d".formatted(index));
        } else if (roll < 0.60) {
            // Plain literal
            return VF.createLiteral("Label for item %d with special chars: <>&\"\\".formatted(index));
        } else if (roll < 0.75) {
            // Language-tagged literal
            return VF.createLiteral("Beschreibung des Elements %d".formatted(index), "de");
        } else if (roll < 0.90) {
            // Typed integer literal
            return VF.createLiteral(index);
        } else {
            // Typed boolean literal
            return VF.createLiteral(index % 2 == 0);
        }
    }

    // --- Result types ---

    private record BenchmarkResult(long statementCount, long durationMs, long memDeltaBytes) {

        double statementsPerSecond() {
            return durationMs > 0 ? (statementCount * 1000.0) / durationMs : 0;
        }

        String memDeltaMb() {
            return "%.1f".formatted(memDeltaBytes / (1024.0 * 1024.0));
        }
    }

    // --- Formatting ---

    private static String formatHeader() {
        return "%-12s | %10s | %16s | %10s | %16s | %10s"
                .formatted("Format", "Statements", "Serializer", "Time (ms)", "Statements/sec", "Mem (MB)");
    }

    private static String formatSeparator() {
        return "-".repeat(91);
    }

    private static String formatRow(String format, int count, String serializer, BenchmarkResult result) {
        return "%-12s | %10d | %16s | %10d | %16.0f | %10s"
                .formatted(
                        format,
                        count,
                        serializer,
                        result.durationMs,
                        result.statementsPerSecond(),
                        result.memDeltaMb());
    }

    private static String formatSpeedupRow(
            String format, int count, String comparison, BenchmarkResult fast, BenchmarkResult other) {
        if (fast.durationMs <= 0 || other.durationMs <= 0) {
            return "%-12s | %10d | %16s | %s".formatted(format, count, comparison, "N/A (too fast to measure)");
        }
        double speedup = (double) other.durationMs / fast.durationMs;
        return "%-12s | %10d | %16s | %10s | %.2fx %s"
                .formatted(format, count, comparison, "", speedup, speedup >= 1.0 ? "faster" : "slower");
    }

    // --- Null OutputStream ---

    /**
     * An {@link OutputStream} that discards all bytes. Used to measure serialization throughput
     * without I/O overhead. Equivalent to writing to {@code /dev/null}.
     */
    private static final class NullOutputStream extends OutputStream {

        static final NullOutputStream INSTANCE = new NullOutputStream();

        @Override
        public void write(int b) {
            // discard
        }

        @Override
        public void write(byte @NonNull [] b) {
            // discard
        }

        @Override
        public void write(byte @NonNull [] b, int off, int len) {
            // discard
        }
    }

    /**
     * A byte-array sink that discards data without throwing {@link java.io.IOException}. Used by
     * the direct per-statement benchmark path where the compiler knows the output never throws.
     */
    private static final class CountingNullOutputStream {

        static CountingNullOutputStream create() {
            return new CountingNullOutputStream();
        }

        @SuppressWarnings("unused")
        void write(byte[] bytes) {
            // discard -- no IOException
        }
    }
}
