package io.carml.testcases.parity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalview.DefaultLogicalViewEvaluatorFactory;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
import io.carml.model.Mapping;
import io.carml.testcases.matcher.IsIsomorphic;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Parity tests that exercise the same mapping under both the reactive default evaluator and the
 * in-process DuckDB evaluator and assert that both produce isomorphic RDF graphs. The tests target
 * file-path resolution semantics across the three documented {@code rml:RelativePathSource}
 * shapes (bare-string source, mapping-directory anchor, working-directory anchor) plus the
 * absolute-path-with-explicit-root case where the absolute path must override {@code rml:root}.
 *
 * <p>Each test writes a small mapping turtle and a CSV data file to a {@link TempDir} and builds
 * both evaluators programmatically against the same {@link Mapping} so that the mapping-directory
 * anchor resolves to the temp directory.
 *
 * <p>A classpath-source parity case is intentionally omitted: the reactive evaluator routes
 * classpath sources through {@code FileResolver}'s {@code classPathBase} branch (loaded via
 * {@code ClassLoader.getResourceAsStream}), while the DuckDB evaluator only knows how to read
 * filesystem paths and remote URLs. The two paths therefore meet at the file-resolution surface
 * for filesystem sources only; classpath parity is not a meaningful comparison without a custom
 * extraction shim that is out of scope here.
 */
class TestRmlEvaluatorParity {

    private static final String DATA_CSV = """
            id,name
            1,alice
            2,bob
            """;

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
    void bareStringSource_reactiveAndDuckDb_produceIsomorphicGraphs(@TempDir Path tempDir) throws IOException {
        // Bare-string sources (rml:source "data.csv", the deprecated shortcut) parse into a
        // CarmlFilePath whose rml:root is unset; both evaluators then default to CWD-relative
        // resolution per FilePathAspects. To exercise the parity contract without polluting the
        // JVM CWD with arbitrary paths, the test allocates a uniquely named sandbox directory
        // under the CWD, references the CSV via that sandbox-relative path, and cleans it up.
        var cwd = Path.of("").toAbsolutePath();
        var sandbox = cwd.resolve("carml-parity-bare-" + System.nanoTime());
        Files.createDirectories(sandbox);
        try {
            var mappingPath = tempDir.resolve("mapping.ttl");
            var sandboxName = sandbox.getFileName().toString();
            Files.writeString(mappingPath, """
                    @prefix ex: <http://example.com/> .
                    @prefix rml: <http://w3id.org/rml/> .

                    <http://ex.com/#TM> a rml:TriplesMap ;
                        rml:logicalSource [ a rml:LogicalSource ;
                            rml:referenceFormulation rml:CSV ;
                            rml:source "%s/data.csv" ] ;
                        rml:subjectMap [ rml:template "http://ex.com/{id}" ] ;
                        rml:predicateObjectMap [ a rml:PredicateObjectMap ;
                            rml:predicateMap [ rml:constant ex:label ] ;
                            rml:objectMap [ rml:reference "name" ] ] .
                    """.formatted(sandboxName), StandardCharsets.UTF_8);
            Files.writeString(sandbox.resolve("data.csv"), DATA_CSV, StandardCharsets.UTF_8);

            assertEvaluatorsProduceIsomorphicOutput(mappingPath, tempDir);
        } finally {
            deleteRecursively(sandbox);
        }
    }

    @Test
    void relativePathWithMappingDirectory_reactiveAndDuckDb_produceIsomorphicGraphs(@TempDir Path tempDir)
            throws IOException {
        var mappingPath = tempDir.resolve("mapping.ttl");
        Files.writeString(mappingPath, """
                @prefix ex: <http://example.com/> .
                @prefix rml: <http://w3id.org/rml/> .

                <http://ex.com/#TM> a rml:TriplesMap ;
                    rml:logicalSource [ a rml:LogicalSource ;
                        rml:referenceFormulation rml:CSV ;
                        rml:source [ a rml:RelativePathSource ;
                            rml:path "data.csv" ;
                            rml:root rml:MappingDirectory ] ] ;
                    rml:subjectMap [ rml:template "http://ex.com/{id}" ] ;
                    rml:predicateObjectMap [ a rml:PredicateObjectMap ;
                        rml:predicateMap [ rml:constant ex:label ] ;
                        rml:objectMap [ rml:reference "name" ] ] .
                """, StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("data.csv"), DATA_CSV, StandardCharsets.UTF_8);

        assertEvaluatorsProduceIsomorphicOutput(mappingPath, tempDir);
    }

    @Test
    void relativePathWithCurrentWorkingDirectory_reactiveAndDuckDb_produceIsomorphicGraphs(@TempDir Path tempDir)
            throws IOException {
        // The data file lives in the CWD-anchored relative path "<tempDir-relative>/data.csv".
        // Computing the JVM CWD at test time and writing the data file into the CWD would pollute
        // the test environment. Instead this test issues a path that resolves correctly only when
        // both evaluators agree on the CWD anchor — using the tempDir as a sub-path that exists
        // relative to CWD via an absolute prefix in rml:path is impossible, so the test takes the
        // simpler approach: it places the data file in the CWD itself under a uniquely named
        // sub-directory and cleans up after itself.
        var cwd = Path.of("").toAbsolutePath();
        var sandbox = cwd.resolve("carml-parity-cwd-" + System.nanoTime());
        Files.createDirectories(sandbox);
        try {
            var mappingPath = tempDir.resolve("mapping.ttl");
            var sandboxName = sandbox.getFileName().toString();
            Files.writeString(mappingPath, """
                    @prefix ex: <http://example.com/> .
                    @prefix rml: <http://w3id.org/rml/> .

                    <http://ex.com/#TM> a rml:TriplesMap ;
                        rml:logicalSource [ a rml:LogicalSource ;
                            rml:referenceFormulation rml:CSV ;
                            rml:source [ a rml:RelativePathSource ;
                                rml:path "%s/data.csv" ;
                                rml:root rml:CurrentWorkingDirectory ] ] ;
                        rml:subjectMap [ rml:template "http://ex.com/{id}" ] ;
                        rml:predicateObjectMap [ a rml:PredicateObjectMap ;
                            rml:predicateMap [ rml:constant ex:label ] ;
                            rml:objectMap [ rml:reference "name" ] ] .
                    """.formatted(sandboxName), StandardCharsets.UTF_8);
            Files.writeString(sandbox.resolve("data.csv"), DATA_CSV, StandardCharsets.UTF_8);

            assertEvaluatorsProduceIsomorphicOutput(mappingPath, tempDir);
        } finally {
            deleteRecursively(sandbox);
        }
    }

    @Test
    void relativeCsvwUrl_reactiveAndDuckDb_anchorAgainstMappingDirectory(@TempDir Path tempDir) throws IOException {
        // A CSVW table with a relative csvw:url anchors against the mapping directory under both
        // evaluators. Mirrors CsvwSourceAspects' PathRelativeTo.MAPPING_DIRECTORY declaration.
        var mappingPath = tempDir.resolve("mapping.ttl");
        Files.writeString(mappingPath, """
                @prefix ex: <http://example.com/> .
                @prefix rml: <http://w3id.org/rml/> .
                @prefix csvw: <http://www.w3.org/ns/csvw#> .

                <http://ex.com/#TM> a rml:TriplesMap ;
                    rml:logicalSource [ a rml:LogicalSource ;
                        rml:referenceFormulation rml:CSV ;
                        rml:source [ a csvw:Table ; csvw:url "data.csv" ] ] ;
                    rml:subjectMap [ rml:template "http://ex.com/{id}" ] ;
                    rml:predicateObjectMap [ a rml:PredicateObjectMap ;
                        rml:predicateMap [ rml:constant ex:label ] ;
                        rml:objectMap [ rml:reference "name" ] ] .
                """, StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("data.csv"), DATA_CSV, StandardCharsets.UTF_8);

        assertEvaluatorsProduceIsomorphicOutput(mappingPath, tempDir);
    }

    @Test
    void absolutePathWithExplicitRoot_reactiveAndDuckDb_produceIsomorphicGraphsAndIgnoreRoot(@TempDir Path tempDir)
            throws IOException {
        // The data file is referenced via an absolute rml:path together with a (deliberately
        // misleading) rml:root rml:MappingDirectory. rml:root is unused for absolute paths; both
        // evaluators must therefore resolve to the absolute path and emit a WARN that the declared
        // root is being ignored. Output parity is the test bar; the WARN is visible in the test
        // logs (slf4j-simple writes to stderr) and is not asserted mechanically here to avoid
        // coupling the test to a logging-binding-specific capture API.
        var dataPath = tempDir.resolve("absolute-data").resolve("data.csv");
        Files.createDirectories(dataPath.getParent());
        Files.writeString(dataPath, DATA_CSV, StandardCharsets.UTF_8);

        var mappingPath = tempDir.resolve("mapping.ttl");
        Files.writeString(mappingPath, """
                @prefix ex: <http://example.com/> .
                @prefix rml: <http://w3id.org/rml/> .

                <http://ex.com/#TM> a rml:TriplesMap ;
                    rml:logicalSource [ a rml:LogicalSource ;
                        rml:referenceFormulation rml:CSV ;
                        rml:source [ a rml:RelativePathSource ;
                            rml:path "%s" ;
                            rml:root rml:MappingDirectory ] ] ;
                    rml:subjectMap [ rml:template "http://ex.com/{id}" ] ;
                    rml:predicateObjectMap [ a rml:PredicateObjectMap ;
                        rml:predicateMap [ rml:constant ex:label ] ;
                        rml:objectMap [ rml:reference "name" ] ] .
                """.formatted(dataPath.toString()), StandardCharsets.UTF_8);

        assertEvaluatorsProduceIsomorphicOutput(mappingPath, tempDir);
    }

    private void assertEvaluatorsProduceIsomorphicOutput(Path mappingPath, Path tempDir) {
        var reactiveResult = runReactive(mappingPath);
        var duckDbResult = runDuckDb(mappingPath);

        // Sanity check: the mapping should not produce an empty graph; both evaluators reading the
        // same source should emit the same number of triples (one per data row).
        assertThat("reactive evaluator returned no triples", reactiveResult.size(), greaterThan(0));
        assertEquals(
                reactiveResult.size(), duckDbResult.size(), "reactive and DuckDb produced different triple counts");
        assertThat(duckDbResult, IsIsomorphic.isIsomorphicTo(reactiveResult));
    }

    private static Model runReactive(Path mappingPath) {
        var mapping = Mapping.of(RDFFormat.TURTLE, mappingPath);
        var mapper = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory())
                .mapping(mapping)
                .triplesMaps(mapping.getTriplesMaps())
                .build();
        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    private Model runDuckDb(Path mappingPath) {
        var mapping = Mapping.of(RDFFormat.TURTLE, mappingPath);
        var mapper = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(new DuckDbLogicalViewEvaluatorFactory(duckDbConnection))
                .mapping(mapping)
                .triplesMaps(mapping.getTriplesMaps())
                .build();
        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var stream = Files.walk(root)) {
            stream.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    // best-effort cleanup; the JVM tempdir reaper will handle leftovers
                }
            });
        }
    }
}
