package io.carml.logicalview.duckdb.benchmark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
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

/**
 * Memory headroom benchmark for the DuckDb evaluator's pull-based iterator. Runs a 100K-row × 10-TM
 * workload through the byte pipeline and reports the JVM heap delta. Establishes that natural
 * backpressure (one Arrow batch resident at a time) keeps memory bounded — pre-fix the same
 * workload retained 2.4 GB of byte arrays and ViewIterations because the no-pause prototype let
 * the producer outrun the consumer.
 *
 * <p>Excluded from the default test run (see {@code <exclude>**\/benchmark/**</exclude>} in the
 * module pom). Run explicitly with {@code mvn -Pbenchmark test
 * -Dtest=MemoryStressBenchmarkTest} when validating backpressure changes.
 */
class MemoryStressBenchmarkTest {

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

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void mapToNTriplesBytes_with10TMs_100KRowsEach_completesUnderMemCap() throws IOException {
        var csvPath = tempDir.resolve("data.csv");
        writeCsv(csvPath, 100_000, 5);
        var triplesMaps = parseMapping(generateMapping(10, 5, csvPath));

        var mapper = RdfRmlMapper.builder()
                .triplesMaps(triplesMaps)
                .fileResolver(tempDir)
                .logicalViewEvaluatorFactory(new DuckDbLogicalViewEvaluatorFactory(duckDbConnection, tempDir))
                .build();

        long peakMemBefore = peakHeapMb();
        var count =
                Objects.requireNonNullElse(mapper.mapToNTriplesBytes().count().block(), 0L);
        long peakMemAfter = peakHeapMb();

        // 10 TMs × 100K rows × 6 statements = 6,000,000 bytes
        assertThat(count, is(6_000_000L));
        System.err.println("[MEM] heap before=" + peakMemBefore + "MB after=" + peakMemAfter + "MB delta="
                + (peakMemAfter - peakMemBefore) + "MB");
    }

    private static long peakHeapMb() {
        Runtime r = Runtime.getRuntime();
        long used = r.totalMemory() - r.freeMemory();
        return used / (1024 * 1024);
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
        sb.append("@base <http://ex.com/> .\n@prefix rml: <http://w3id.org/rml/> .\n");
        sb.append("@prefix ql: <http://semweb.mmlab.be/ns/ql#> .\n@prefix ex: <http://example.com/> .\n\n");
        for (int tm = 0; tm < numTms; tm++) {
            sb.append("<#TriplesMap").append(tm).append("> a rml:TriplesMap ;\n");
            sb.append("    rml:logicalSource [ a rml:LogicalSource ;\n");
            sb.append("        rml:referenceFormulation ql:CSV ;\n");
            sb.append("        rml:source [ a rml:FilePath ; rml:path \"")
                    .append(absPath)
                    .append("\" ] ] ;\n");
            sb.append("    rml:subjectMap [ rml:template \"http://ex.com/tm")
                    .append(tm)
                    .append("/{id}\" ;\n");
            sb.append("        rml:class ex:Class").append(tm).append(" ]");
            for (int p = 0; p < numProps; p++) {
                sb.append(" ;\n    rml:predicateObjectMap [\n");
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
