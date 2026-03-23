package io.carml.testcases.rml;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
import io.carml.model.TriplesMap;
import io.carml.testcases.model.TestCase;
import io.carml.util.RmlMappingLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for conformance test suites that run against the DuckDB evaluator. Manages a shared
 * in-memory DuckDB connection and overrides {@link #executeMapping} to use
 * {@link DuckDbLogicalViewEvaluatorFactory}.
 */
public abstract class DuckDbTestCaseSuite extends RmlTestCaseSuite {

    private Connection connection;

    protected Connection getConnection() {
        return connection;
    }

    @BeforeAll
    void setUpDuckDb() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    void tearDownDuckDb() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        // Drop source cache tables from previous test case to prevent stale data collisions
        dropSourceCacheTables();

        var evaluatorFactory = new DuckDbLogicalViewEvaluatorFactory(connection);

        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(evaluatorFactory);

        Optional.ofNullable(testCase.getDefaultBaseIri()).or(this::getBaseIri).ifPresent(mapperBuilder::baseIri);

        var mappingStream =
                getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument());
        Set<TriplesMap> mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        RdfRmlMapper mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    /**
     * Drops all source cache tables (regular tables with the {@code __carml_src_} prefix) created by
     * previous test cases. The source table cache creates regular tables so they are visible across
     * duplicated connections; this cleanup prevents stale data from leaking across test cases.
     */
    private void dropSourceCacheTables() {
        try (var stmt = connection.createStatement();
                var rs = stmt.executeQuery(
                        "SELECT table_name FROM memory.information_schema.tables WHERE table_name LIKE '__carml_src_%'")) {
            while (rs.next()) {
                var tableName = rs.getString(1);
                try (var dropStmt = connection.createStatement()) {
                    dropStmt.execute("DROP TABLE IF EXISTS memory.main.\"%s\"".formatted(tableName));
                }
            }
        } catch (java.sql.SQLException e) {
            // Ignore — best effort cleanup
        }
    }
}
