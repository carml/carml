package io.carml.logicalview.duckdb.benchmark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
import io.carml.util.RmlMappingLoader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.scheduler.Schedulers;

/**
 * Regression benchmark replicating the KGCW 2026 Track 2 {@code mappings_30_5} cell using the
 * actual benchmark input emitted by the KGCW Python generator
 * ({@code kg-construct/kgc-challenge/track_2_performance/bench_generator/mappings.py}, configured
 * via {@code resources/scenarios-config/benchmark-mappings.json#30TM5POM}).
 *
 * <p>The CSV ({@code data.csv}) and turtle mapping ({@code mapping.rml.ttl}) are read verbatim
 * from {@code KGCW_INPUT_DIR} (default {@code /tmp/kgcw_mappings_30_5/CARML/csv/mappings_30_5/data/shared/}).
 * Regenerate with:
 * <pre>{@code
 * cd kg-construct/kgc-challenge/track_2_performance && source .venv/bin/activate && python -c "
 * import sys; sys.path.insert(0, '.')
 * from bench_generator.mappings import Mappings
 * import os; out = '/tmp/kgcw_mappings_30_5'; os.makedirs(out, exist_ok=True)
 * Mappings(main_directory=out, verbose=False, number_of_tms=30, number_of_poms=5,
 *          number_of_members=100000, number_of_properties=30, value_size=0,
 *          data_format='csv', engine='CARML').generate()
 * "
 * }</pre>
 *
 * <p>The mapping references {@code /data/shared/data.csv} (the production Docker mount path).
 * For the local test we rewrite that to a path resolvable in the test temp dir before parsing.
 *
 * <p>Excluded from the default test run via {@code <exclude>**\/benchmark/**</exclude>}; run
 * explicitly with {@code mvn -Pbenchmark test -pl carml-logical-view-duckdb
 * -Dtest=MultiTmScaleRegressionBenchmarkTest}.
 */
class MultiTmScaleRegressionBenchmarkTest {

    private static final Path KGCW_INPUT_DIR = Path.of(
            System.getProperty("carml.kgcw.inputDir", "/tmp/kgcw_mappings_30_5/CARML/csv/mappings_30_5/data/shared"));

    private static final long EXPECTED_TRIPLES = 30L * 100_000 * 5;

    @TempDir
    Path tempDir;

    @Test
    @Timeout(value = 1200, unit = TimeUnit.SECONDS)
    void mapToNTriplesBytes_kgcwMappings30_5_producesFullOutput() throws IOException {
        var csvSource = KGCW_INPUT_DIR.resolve("data.csv");
        var mappingSource = KGCW_INPUT_DIR.resolve("mapping.rml.ttl");
        if (!Files.isRegularFile(csvSource) || !Files.isRegularFile(mappingSource)) {
            throw new IllegalStateException("KGCW input not found at " + KGCW_INPUT_DIR
                    + " — regenerate via the Python generator (see Javadoc).");
        }

        // Stage the CSV at tempDir/data.csv and rewrite the mapping's hard-coded
        // /data/shared/data.csv path to "data.csv" so DuckDb's file_search_path resolves it.
        var csvDest = tempDir.resolve("data.csv");
        Files.copy(csvSource, csvDest);
        var mappingTurtle =
                Files.readString(mappingSource, StandardCharsets.UTF_8).replace("/data/shared/data.csv", "data.csv");
        var mappingDest = tempDir.resolve("mapping.rml.ttl");
        Files.writeString(mappingDest, mappingTurtle, StandardCharsets.UTF_8);

        var triplesMaps = RmlMappingLoader.build().load(RDFFormat.TURTLE, Files.newInputStream(mappingDest));

        System.err.printf(
                "[ENV] Xmx=%dMB cores=%d csv=%dKB mapping=%dKB tms=%d%n",
                Runtime.getRuntime().maxMemory() / (1024 * 1024),
                Runtime.getRuntime().availableProcessors(),
                Files.size(csvDest) / 1024,
                Files.size(mappingDest) / 1024,
                triplesMaps.size());

        var unboundedScheduler = Schedulers.fromExecutorService(Executors.newCachedThreadPool(), "duckdb-vt");
        try (var factory = DuckDbLogicalViewEvaluatorFactory.createOnDisk(unboundedScheduler, "1GB")) {
            factory.setFileBasePath(tempDir);
            var mapper = RdfRmlMapper.builder()
                    .triplesMaps(triplesMaps)
                    .fileResolver(tempDir)
                    .logicalViewEvaluatorFactory(factory)
                    .build();

            long start = System.nanoTime();
            var count = Objects.requireNonNullElse(
                    mapper.mapToNTriplesBytes().count().block(), 0L);
            long durationSec = (System.nanoTime() - start) / 1_000_000_000L;

            System.err.printf(
                    "[REGRESSION] mappings_30_5 → %d triples (expected %d) in %ds%n",
                    count, EXPECTED_TRIPLES, durationSec);

            assertThat(count, is(EXPECTED_TRIPLES));
        }
    }
}
