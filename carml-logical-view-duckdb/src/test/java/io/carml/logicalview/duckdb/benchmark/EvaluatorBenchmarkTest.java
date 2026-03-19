package io.carml.logicalview.duckdb.benchmark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalview.DefaultLogicalViewEvaluatorFactory;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
import io.carml.model.TriplesMap;
import io.carml.util.RmlMappingLoader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * Performance benchmark comparing the DuckDB evaluator against the reactive (default) evaluator.
 *
 * <p>Measures end-to-end wall time and output throughput (triples/second) across JSON and CSV
 * formats at various data scales (100, 1K, 10K, 100K records). Each scenario runs the same RML
 * mapping through both evaluators via {@link RdfRmlMapper#map()}, collecting all statements and
 * measuring elapsed time.
 *
 * <p>This class is placed in the {@code benchmark} package which is excluded from normal test runs
 * via surefire's {@code <excludes>} configuration in the module POM. Run explicitly with the
 * {@code benchmark} Maven profile:
 *
 * <pre>{@code
 * mvn test -pl carml-logical-view-duckdb -Dcheckstyle.skip=true -Pbenchmark
 * }</pre>
 *
 * <p><strong>Warm-up:</strong> Each scenario includes a warm-up pass (100 records) to trigger JIT
 * compilation and DuckDB query planning caches before the measured run.
 *
 * <p><strong>Memory:</strong> Peak heap usage is sampled via {@link Runtime#totalMemory()} minus
 * {@link Runtime#freeMemory()} before and after each run. This is an approximation — for precise
 * memory profiling, use a dedicated tool (async-profiler, JFR).
 *
 * <p><strong>Note:</strong> Results vary with hardware, JVM version, and background load. The
 * benchmark is designed for relative comparison between evaluators on the same machine, not for
 * absolute throughput numbers.
 */
@Slf4j
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
class EvaluatorBenchmarkTest {

    private static final int WARM_UP_RECORDS = 100;
    private static final int[] RECORD_COUNTS = {100, 1_000, 10_000, 100_000};
    private static final int TAGS_PER_RECORD = 5;

    @TempDir
    static Path tempDir;

    private Connection duckDbConnection;

    @BeforeAll
    void setUp() throws SQLException {
        duckDbConnection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    void tearDown() throws SQLException {
        if (duckDbConnection != null && !duckDbConnection.isClosed()) {
            duckDbConnection.close();
        }
    }

    // --- JSON benchmarks ---

    @Test
    void benchmark_json_allScales() throws IOException {
        LOG.info("=== JSON Benchmark ===");
        LOG.info("{}", formatHeader());

        // Warm up both evaluators
        var warmUpPath = BenchmarkDataGenerator.generateJson(tempDir, WARM_UP_RECORDS);
        var warmUpMapping =
                BenchmarkMappingGenerator.jsonMapping(warmUpPath.getFileName().toString());
        runMapping(warmUpMapping, "duckdb");
        runMapping(warmUpMapping, "reactive");

        for (int count : RECORD_COUNTS) {
            var dataPath = BenchmarkDataGenerator.generateJson(tempDir, count);
            var mapping =
                    BenchmarkMappingGenerator.jsonMapping(dataPath.getFileName().toString());

            var duckDbResult = runMapping(mapping, "duckdb");
            var reactiveResult = runMapping(mapping, "reactive");

            LOG.info("{}", formatRow("JSON", count, "DuckDB", duckDbResult));
            LOG.info("{}", formatRow("JSON", count, "Reactive", reactiveResult));
            LOG.info("{}", formatSpeedupRow("JSON", count, duckDbResult.durationMs, reactiveResult.durationMs));
            LOG.info("");

            // Sanity check: both evaluators produce the same number of triples
            assertThat(
                    "DuckDB should produce triples for JSON %d records".formatted(count),
                    duckDbResult.tripleCount,
                    greaterThan(0L));
            assertThat(
                    "Reactive should produce triples for JSON %d records".formatted(count),
                    reactiveResult.tripleCount,
                    greaterThan(0L));
        }
    }

    // --- CSV benchmarks ---

    @Test
    void benchmark_csv_allScales() throws IOException {
        LOG.info("=== CSV Benchmark ===");
        LOG.info("{}", formatHeader());

        // Warm up both evaluators
        var warmUpPath = BenchmarkDataGenerator.generateCsv(tempDir, WARM_UP_RECORDS);
        var warmUpMapping =
                BenchmarkMappingGenerator.csvMapping(warmUpPath.getFileName().toString());
        runMapping(warmUpMapping, "duckdb");
        runMapping(warmUpMapping, "reactive");

        for (int count : RECORD_COUNTS) {
            var dataPath = BenchmarkDataGenerator.generateCsv(tempDir, count);
            var mapping =
                    BenchmarkMappingGenerator.csvMapping(dataPath.getFileName().toString());

            var duckDbResult = runMapping(mapping, "duckdb");
            var reactiveResult = runMapping(mapping, "reactive");

            LOG.info("{}", formatRow("CSV", count, "DuckDB", duckDbResult));
            LOG.info("{}", formatRow("CSV", count, "Reactive", reactiveResult));
            LOG.info("{}", formatSpeedupRow("CSV", count, duckDbResult.durationMs, reactiveResult.durationMs));
            LOG.info("");

            assertThat(
                    "DuckDB should produce triples for CSV %d records".formatted(count),
                    duckDbResult.tripleCount,
                    greaterThan(0L));
            assertThat(
                    "Reactive should produce triples for CSV %d records".formatted(count),
                    reactiveResult.tripleCount,
                    greaterThan(0L));
        }
    }

    // --- Per-phase profiling ---

    @Test
    void benchmark_csv_phaseBreakdown() throws IOException {
        LOG.info("=== CSV Per-Phase Breakdown ===");
        LOG.info(
                "{}",
                "%-14s | %8s | %10s | %12s | %12s | %12s"
                        .formatted("Format", "Records", "Evaluator", "Count(ms)", "Model(ms)", "Total(ms)"));
        LOG.info(
                "{}",
                "Count = separate run: eval + RDF gen (no Model collect). "
                        + "Model ~ Total - Count (two independent runs; approximate).");

        // Warm up both evaluators and both measurement modes
        var warmUpPath = BenchmarkDataGenerator.generateCsv(tempDir, WARM_UP_RECORDS);
        var warmUpMapping =
                BenchmarkMappingGenerator.csvMapping(warmUpPath.getFileName().toString());
        runMapping(warmUpMapping, "duckdb");
        runCountOnly(warmUpMapping, "duckdb");
        runMapping(warmUpMapping, "reactive");
        runCountOnly(warmUpMapping, "reactive");

        for (int count : RECORD_COUNTS) {
            var dataPath = BenchmarkDataGenerator.generateCsv(tempDir, count);
            var mapping =
                    BenchmarkMappingGenerator.csvMapping(dataPath.getFileName().toString());

            for (var evaluator : new String[] {"duckdb", "reactive"}) {
                var countResult = runCountOnly(mapping, evaluator);
                var fullResult = runMapping(mapping, evaluator);

                long countMs = countResult.durationMs;
                long modelMs = Math.max(0, fullResult.durationMs - countResult.durationMs);

                LOG.info(
                        "{}",
                        "%-14s | %8d | %10s | %12d | %12d | %12d"
                                .formatted(
                                        "CSV",
                                        count,
                                        evaluator.equals("duckdb") ? "DuckDB" : "Reactive",
                                        countMs,
                                        modelMs,
                                        fullResult.durationMs));
            }
            LOG.info("");
        }
    }

    // --- Shared-source benchmarks (single-read cache) ---

    @Test
    void benchmark_json_sharedSource() throws IOException {
        LOG.info("=== JSON Shared Source (2 TriplesMap on same file) ===");
        LOG.info(
                "{}",
                "%-14s | %8s | %10s | %8s | %12s | %8s"
                        .formatted("Format", "Records", "Evaluator", "Time(ms)", "Triples/sec", "Mem(MB)"));

        // Warm up all paths
        var warmUpPath = BenchmarkDataGenerator.generateJson(tempDir, WARM_UP_RECORDS);
        var warmUpMapping = BenchmarkMappingGenerator.sharedSourceJsonMapping(
                warmUpPath.getFileName().toString());
        runMapping(warmUpMapping, "duckdb");
        runMapping(warmUpMapping, "reactive");

        for (int count : RECORD_COUNTS) {
            var dataPath = BenchmarkDataGenerator.generateJson(tempDir, count);
            var mapping = BenchmarkMappingGenerator.sharedSourceJsonMapping(
                    dataPath.getFileName().toString());

            var duckDbResult = runMapping(mapping, "duckdb");
            var reactiveResult = runMapping(mapping, "reactive");

            LOG.info("{}", formatRow("JSON 2TM", count, "DuckDB", duckDbResult));
            LOG.info("{}", formatRow("JSON 2TM", count, "Reactive", reactiveResult));
            LOG.info("{}", formatSpeedupRow("JSON 2TM", count, duckDbResult.durationMs, reactiveResult.durationMs));
            LOG.info("");

            assertThat(duckDbResult.tripleCount(), greaterThan(0L));
            assertThat(reactiveResult.tripleCount(), greaterThan(0L));
        }
    }

    // --- Nested JSON (iterable field / UNNEST) benchmarks ---
    // Note: only DuckDB is benchmarked here. The reactive evaluator's iterable field evaluation
    // throws ClassCastException for self-referencing expressions (rml:reference "$") in iterable
    // fields because the JSurfer resolver returns String while the pipeline expects JsonNode.
    // This is a known limitation of the reactive path.

    @Test
    void benchmark_nestedJson_duckDbOnly() throws IOException {
        LOG.info("=== Nested JSON (Iterable Fields) Benchmark — DuckDB only ===");
        LOG.info("{}", formatHeader());

        // Warm up
        var warmUpPath = BenchmarkDataGenerator.generateJsonWithNested(tempDir, WARM_UP_RECORDS, TAGS_PER_RECORD);
        var warmUpMapping = BenchmarkMappingGenerator.nestedJsonMapping(
                warmUpPath.getFileName().toString());
        runMapping(warmUpMapping, "duckdb");

        for (int count : RECORD_COUNTS) {
            var dataPath = BenchmarkDataGenerator.generateJsonWithNested(tempDir, count, TAGS_PER_RECORD);
            var mapping = BenchmarkMappingGenerator.nestedJsonMapping(
                    dataPath.getFileName().toString());

            var duckDbResult = runMapping(mapping, "duckdb");

            LOG.info("{}", formatRow("Nested JSON", count, "DuckDB", duckDbResult));
            LOG.info("");

            assertThat(
                    "DuckDB should produce triples for nested JSON %d records".formatted(count),
                    duckDbResult.tripleCount,
                    greaterThan(0L));
        }
    }

    // --- Mapping execution ---

    private BenchmarkResult runMapping(String turtleMapping, String evaluatorMode) {
        var mappingStream = new ByteArrayInputStream(turtleMapping.getBytes(StandardCharsets.UTF_8));
        Set<TriplesMap> triplesMaps = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .triplesMaps(triplesMaps)
                .fileResolver(tempDir);

        if ("duckdb".equals(evaluatorMode)) {
            mapperBuilder.logicalViewEvaluatorFactory(new DuckDbLogicalViewEvaluatorFactory(duckDbConnection, tempDir));
        } else {
            mapperBuilder.logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory());
        }

        RdfRmlMapper mapper = mapperBuilder.build();

        // Force GC before measurement to reduce noise
        System.gc();
        var runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        long startNanos = System.nanoTime();
        var model = mapper.map().collect(ModelCollector.toModel()).block();
        long tripleCount = model != null ? model.size() : 0;
        long endNanos = System.nanoTime();

        long memAfter = runtime.totalMemory() - runtime.freeMemory();
        long durationMs = (endNanos - startNanos) / 1_000_000;
        long memDeltaBytes = Math.max(0, memAfter - memBefore);

        return new BenchmarkResult(tripleCount, durationMs, memDeltaBytes);
    }

    /**
     * Measures evaluator + RDF generation: map() → count statements.
     * No Model collection overhead.
     */
    private BenchmarkResult runCountOnly(String turtleMapping, String evaluatorMode) {
        var mappingStream = new ByteArrayInputStream(turtleMapping.getBytes(StandardCharsets.UTF_8));
        Set<TriplesMap> triplesMaps = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .triplesMaps(triplesMaps)
                .fileResolver(tempDir);

        if ("duckdb".equals(evaluatorMode)) {
            mapperBuilder.logicalViewEvaluatorFactory(new DuckDbLogicalViewEvaluatorFactory(duckDbConnection, tempDir));
        } else {
            mapperBuilder.logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory());
        }

        RdfRmlMapper mapper = mapperBuilder.build();

        System.gc();
        long startNanos = System.nanoTime();
        long statementCount = Objects.requireNonNullElse(mapper.map().count().block(), 0L);
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000;

        return new BenchmarkResult(statementCount, durationMs, 0);
    }

    /**
     * Measures evaluator + byte encoding: mapToNTriplesBytes() → count byte arrays.
     * No Statement objects created, no Model collection.
     */
    private BenchmarkResult runBytesOnly(String turtleMapping, String evaluatorMode) {
        var mappingStream = new ByteArrayInputStream(turtleMapping.getBytes(StandardCharsets.UTF_8));
        Set<TriplesMap> triplesMaps = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .triplesMaps(triplesMaps)
                .fileResolver(tempDir);

        if ("duckdb".equals(evaluatorMode)) {
            mapperBuilder.logicalViewEvaluatorFactory(new DuckDbLogicalViewEvaluatorFactory(duckDbConnection, tempDir));
        } else {
            mapperBuilder.logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory());
        }

        RdfRmlMapper mapper = mapperBuilder.build();

        System.gc();
        long startNanos = System.nanoTime();
        long byteArrayCount =
                Objects.requireNonNullElse(mapper.mapToNTriplesBytes().count().block(), 0L);
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000;

        return new BenchmarkResult(byteArrayCount, durationMs, 0);
    }

    // --- Statement vs Bytes comparison ---

    @Test
    void benchmark_csv_statementVsBytes() throws IOException {
        LOG.info("=== CSV Statement vs Bytes Pipeline ===");
        LOG.info(
                "{}",
                "%-14s | %8s | %16s | %8s | %12s"
                        .formatted("Format", "Records", "Pipeline", "Time(ms)", "Triples/sec"));

        // Warm up
        var warmUpPath = BenchmarkDataGenerator.generateCsv(tempDir, WARM_UP_RECORDS);
        var warmUpMapping =
                BenchmarkMappingGenerator.csvMapping(warmUpPath.getFileName().toString());
        runCountOnly(warmUpMapping, "duckdb");
        runBytesOnly(warmUpMapping, "duckdb");
        runCountOnly(warmUpMapping, "reactive");
        runBytesOnly(warmUpMapping, "reactive");

        for (int count : RECORD_COUNTS) {
            var dataPath = BenchmarkDataGenerator.generateCsv(tempDir, count);
            var mapping =
                    BenchmarkMappingGenerator.csvMapping(dataPath.getFileName().toString());

            var duckStmt = runCountOnly(mapping, "duckdb");
            var duckBytes = runBytesOnly(mapping, "duckdb");
            var reactStmt = runCountOnly(mapping, "reactive");
            var reactBytes = runBytesOnly(mapping, "reactive");

            LOG.info(
                    "{}",
                    "%-14s | %8d | %16s | %8d | %12.0f"
                            .formatted(
                                    "CSV", count, "DuckDB Stmt", duckStmt.durationMs(), duckStmt.triplesPerSecond()));
            LOG.info(
                    "{}",
                    "%-14s | %8d | %16s | %8d | %12.0f"
                            .formatted(
                                    "CSV",
                                    count,
                                    "DuckDB Bytes",
                                    duckBytes.durationMs(),
                                    duckBytes.triplesPerSecond()));
            LOG.info(
                    "{}",
                    "%-14s | %8d | %16s | %8d | %12.0f"
                            .formatted(
                                    "CSV",
                                    count,
                                    "Reactive Stmt",
                                    reactStmt.durationMs(),
                                    reactStmt.triplesPerSecond()));
            LOG.info(
                    "{}",
                    "%-14s | %8d | %16s | %8d | %12.0f"
                            .formatted(
                                    "CSV",
                                    count,
                                    "Reactive Bytes",
                                    reactBytes.durationMs(),
                                    reactBytes.triplesPerSecond()));

            if (duckStmt.durationMs() > 0 && duckBytes.durationMs() > 0) {
                LOG.info(
                        "{}",
                        "%-14s | %8d | %16s | %.2fx"
                                .formatted(
                                        "CSV",
                                        count,
                                        "Duck Stmt/Bytes",
                                        (double) duckStmt.durationMs() / duckBytes.durationMs()));
            }
            if (reactStmt.durationMs() > 0 && reactBytes.durationMs() > 0) {
                LOG.info(
                        "{}",
                        "%-14s | %8d | %16s | %.2fx"
                                .formatted(
                                        "CSV",
                                        count,
                                        "React Stmt/Bytes",
                                        (double) reactStmt.durationMs() / reactBytes.durationMs()));
            }
            LOG.info("");

            assertThat(duckBytes.tripleCount(), greaterThan(0L));
            assertThat(reactBytes.tripleCount(), greaterThan(0L));
        }
    }

    // --- Result types ---

    private record BenchmarkResult(long tripleCount, long durationMs, long memDeltaBytes) {

        double triplesPerSecond() {
            return durationMs > 0 ? (tripleCount * 1000.0) / durationMs : 0;
        }

        String memDeltaMb() {
            return "%.1f".formatted(memDeltaBytes / (1024.0 * 1024.0));
        }
    }

    // --- Formatting ---

    private static String formatHeader() {
        return "%-14s | %8s | %10s | %8s | %12s | %8s"
                .formatted("Format", "Records", "Evaluator", "Time(ms)", "Triples/sec", "Mem(MB)");
    }

    private static String formatRow(String format, int records, String evaluator, BenchmarkResult result) {
        return "%-14s | %8d | %10s | %8d | %12.0f | %8s"
                .formatted(
                        format, records, evaluator, result.durationMs, result.triplesPerSecond(), result.memDeltaMb());
    }

    private static String formatSpeedupRow(String format, int records, long duckDbMs, long reactiveMs) {
        if (duckDbMs <= 0 || reactiveMs <= 0) {
            return "%-14s | %8d | %10s | %s".formatted(format, records, "Speedup", "N/A (too fast to measure)");
        }
        double speedup = (double) reactiveMs / duckDbMs;
        String direction = speedup >= 1.0 ? "DuckDB faster" : "Reactive faster";
        return "%-14s | %8d | %10s | %.2fx (%s)".formatted(format, records, "Speedup", speedup, direction);
    }
}
