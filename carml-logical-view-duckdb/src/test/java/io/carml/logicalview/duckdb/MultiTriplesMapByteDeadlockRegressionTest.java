package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalview.DefaultLogicalViewEvaluatorFactory;
import io.carml.util.RmlMappingLoader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Regression test for the byte-pipeline deadlock with high TriplesMap concurrency.
 */
class MultiTriplesMapByteDeadlockRegressionTest {

    @TempDir
    Path tempDir;

    private Connection duckDbConnection;

    @BeforeEach
    void setUp() throws SQLException {
        duckDbConnection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (duckDbConnection != null && !duckDbConnection.isClosed()) {
            duckDbConnection.close();
        }
    }

    /**
     * Diagnostic ladder for the byte pipeline through the DuckDb evaluator. Single-TM is the
     * baseline (test scaffolding works); 4-TM detects deadlocks at low fan-in; 30-TM mirrors the
     * KGCW {@code mappings_30_5} shape at smaller scale: {@code numTms × 1000 rows × (1 rdf:type +
     * 5 PoMs)} byte arrays through the byte pipeline.
     */
    @ParameterizedTest(name = "{0} TriplesMaps → {1} bytes")
    @CsvSource({"1, 6000", "4, 24000", "30, 180000"})
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void mapToNTriplesBytes_onDuckDbEvaluator_completes(int numTms, long expectedCount) throws IOException {
        var csvPath = tempDir.resolve("data.csv");
        writeCsv(csvPath, 1_000, 5);
        var triplesMaps = parseMapping(generateMapping(numTms, 5, csvPath));

        var mapper = RdfRmlMapper.builder()
                .triplesMaps(triplesMaps)
                .fileResolver(tempDir)
                .logicalViewEvaluatorFactory(new DuckDbLogicalViewEvaluatorFactory(duckDbConnection, tempDir))
                .build();

        var count =
                Objects.requireNonNullElse(mapper.mapToNTriplesBytes().count().block(), 0L);

        assertThat(count, is(expectedCount));
    }

    /**
     * 30-TM through the REACTIVE evaluator (no DuckDb). Disambiguates whether the deadlock is in
     * the engine's outer flatMap (would hang here too) or DuckDb-specific (would pass here).
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void mapToNTriplesBytes_with30TriplesMapsOnReactiveEvaluator_completes() throws IOException {
        // Reactive evaluator resolves files via the resolver chain; absolute paths in
        // rml:FilePath get re-resolved against fileResolver(tempDir) producing a doubled path.
        // Use root "/" so absolute paths pass through unchanged.
        var csvPath = tempDir.resolve("data.csv");
        writeCsv(csvPath, 1_000, 5);
        var triplesMaps = parseMapping(generateMapping(30, 5, csvPath));

        var mapper = RdfRmlMapper.builder()
                .triplesMaps(triplesMaps)
                .fileResolver(java.nio.file.Path.of("/"))
                .logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory())
                .build();

        var count =
                Objects.requireNonNullElse(mapper.mapToNTriplesBytes().count().block(), 0L);

        assertThat(count, is(180_000L));
    }

    /**
     * Same 30-TM scenario but through the STATEMENT pipeline (mapper.map().count().block()) on
     * DuckDb evaluator. Confirms whether the bug is byte-pipeline-specific or affects the
     * Statement pipeline too.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void map_with30TriplesMapsOnDuckDbEvaluator_completes() throws IOException {
        var csvPath = tempDir.resolve("data.csv");
        writeCsv(csvPath, 1_000, 5);
        var triplesMaps = parseMapping(generateMapping(30, 5, csvPath));

        var mapper = RdfRmlMapper.builder()
                .triplesMaps(triplesMaps)
                .fileResolver(tempDir)
                .logicalViewEvaluatorFactory(new DuckDbLogicalViewEvaluatorFactory(duckDbConnection, tempDir))
                .build();

        var count = Objects.requireNonNullElse(mapper.map().count().block(), 0L);

        assertThat(count, is(180_000L));
    }

    private static void writeCsv(Path path, int rows, int numProps) throws IOException {
        var sb = new StringBuilder();
        sb.append("id");
        for (int p = 0; p < numProps; p++) {
            sb.append(",p").append(p);
        }
        sb.append('\n');
        for (int i = 0; i < rows; i++) {
            sb.append(i);
            for (int p = 0; p < numProps; p++) {
                sb.append(",v").append(p).append('_').append(i);
            }
            sb.append('\n');
        }
        Files.writeString(path, sb.toString(), StandardCharsets.UTF_8);
    }

    private static String generateMapping(int numTms, int numProps, Path csvPath) {
        var absPath = csvPath.toAbsolutePath().toString();
        var sb = new StringBuilder();
        sb.append("@base <http://ex.com/> .\n");
        sb.append("@prefix rml: <http://w3id.org/rml/> .\n");
        sb.append("@prefix ql: <http://semweb.mmlab.be/ns/ql#> .\n");
        sb.append("@prefix ex: <http://example.com/> .\n\n");

        for (int tm = 0; tm < numTms; tm++) {
            sb.append("<#TriplesMap").append(tm).append("> a rml:TriplesMap ;\n");
            sb.append("    rml:logicalSource [ a rml:LogicalSource ;\n");
            sb.append("        rml:referenceFormulation ql:CSV ;\n");
            sb.append("        rml:source [ a rml:FilePath ;\n");
            sb.append("            rml:path \"").append(absPath).append("\" ] ] ;\n");
            sb.append("    rml:subjectMap [ rml:template \"http://ex.com/tm")
                    .append(tm)
                    .append("/{id}\" ;\n");
            sb.append("        rml:class ex:Class").append(tm).append(" ]");
            for (int p = 0; p < numProps; p++) {
                sb.append(" ;\n");
                sb.append("    rml:predicateObjectMap [\n");
                sb.append("        rml:predicateMap [ rml:constant ex:p")
                        .append(p)
                        .append(" ] ;\n");
                sb.append("        rml:objectMap [ rml:reference \"p").append(p).append("\" ] ]");
            }
            sb.append(" .\n\n");
        }

        return sb.toString();
    }

    private static java.util.Set<io.carml.model.TriplesMap> parseMapping(String turtle) {
        var stream = new ByteArrayInputStream(turtle.getBytes(StandardCharsets.UTF_8));
        return RmlMappingLoader.build().load(RDFFormat.TURTLE, stream);
    }
}
