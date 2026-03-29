package io.carml.testcases.rml.ioregistry;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
import io.carml.model.DatabaseSource;
import io.carml.model.TriplesMap;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.DuckDbTestCaseSuite;
import io.carml.util.RmlMappingLoader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Runs all W3C RML IO-Registry conformance tests against DuckDB. Non-SQL tests (JSON, XML, CSV)
 * run against DuckDB's native evaluator. MySQL tests (RMLIOREGTC0004*) run against a real MySQL
 * container via DuckDB's {@code mysql} scanner extension, and PostgreSQL tests (RMLIOREGTC0005*)
 * run against a real PostgreSQL container via DuckDB's {@code postgres} scanner extension.
 *
 * <p>The mapping's {@code CONNECTIONDSN} placeholder is substituted with the real JDBC URL at
 * runtime. The {@link DuckDbLogicalViewEvaluatorFactory}'s internal {@code DuckDbDatabaseAttacher}
 * parses the JDBC DSN, INSTALLs/LOADs the scanner extension, and ATTACHes the database
 * automatically. SQL source handlers then emit fully qualified table names (e.g.,
 * {@code "<catalog>"."<schema>"."<table>"}), eliminating the need for {@code USE} commands.
 *
 * <p>For each SQL test case, the {@code resource.sql} is loaded into the appropriate database via
 * JDBC, then the database attachment is refreshed via the attacher to pick up schema changes
 * (DROPped/CREATEd tables).
 */
@Slf4j
class TestRmlIoRegistryTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("test")
            .withUsername("root")
            .withPassword("test");

    @Container
    private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("test")
            .withUsername("postgres")
            .withPassword("test");

    @Override
    protected String getBasePath() {
        return "/rml/ioregistry/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // --- Test case bugs ---

                // JSON: $.THIS_VALUE_DOES_NOT_EXIST -> NULL, not error. The JSONPath IO-Registry spec
                // (section "Generation of null values") states that selectors referring to non-existent
                // JSON names result in NULL.
                "RMLIOREGTC0002b",

                // --- DuckDB limitations ---

                // XML/XPath sources: DuckDB has no native XML support
                "RMLIOREGTC0003",

                // SQL Server: no SQL Server container configured
                "RMLIOREGTC0006",

                // Test case bug: expected output has plain "33" for JSON integer; engine produces
                // "33"^^xsd:integer
                "RMLIOREGTC0007a",

                // --- Unsupported source types ---

                // WoT (td:Thing) source descriptions
                "RMLIOREGTC0008a", // HTTP JSON API
                "RMLIOREGTC0009a", // Kafka stream
                "RMLIOREGTC0010a", // MQTT stream

                // SPARQL endpoint (sd:Service)
                "RMLIOREGTC0011a",

                // --- CSVW issues ---

                // CSVW with HTTP URL source: triggers reactor-netty HTTP fetch which fails due to
                // Netty 4.2 MultiThreadIoEventLoopGroup not on classpath
                "RMLIOREGTC0012a",

                // UTF-16 encoding not supported by DuckDB's read_csv
                "RMLIOREGTC0012f",

                // Case-sensitive column name mismatch: CSV has lowercase headers, mapping references
                // uppercase. DuckDB column names are case-sensitive.
                "RMLIOREGTC0012i",

                // --- MySQL DuckDB scanner issues ---

                // DuckDB natural type inference: produces typed literals where tests expect plain
                "RMLIOREGTC0004k",
                "RMLIOREGTC0004o",
                "RMLIOREGTC0004t",
                "RMLIOREGTC0004w",
                "RMLIOREGTC0004z",

                // SQL2008Query references non-existent column; DuckDB rejects
                "RMLIOREGTC0004l",

                // CONCAT with backtick identifiers: jOOQ quotes column refs case-sensitively but
                // DuckDB scanner exposes MySQL columns with lowercased names
                "RMLIOREGTC0004n",

                // DuckDB BLOB handling: binary literal value not supported
                "RMLIOREGTC0004x",

                // --- PostgreSQL DuckDB scanner issues ---

                // DuckDB natural type inference: produces typed literals where tests expect plain
                "RMLIOREGTC0005d",
                "RMLIOREGTC0005k",
                "RMLIOREGTC0005o",
                "RMLIOREGTC0005t",
                "RMLIOREGTC0005w",
                "RMLIOREGTC0005z",

                // SQL2008Query references non-existent column; DuckDB rejects
                "RMLIOREGTC0005l",

                // DuckDB BLOB handling: binary literal value not supported
                "RMLIOREGTC0005x");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var sqlStream = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, "resource.sql");
        if (sqlStream == null) {
            // Non-SQL test case: use default DuckDB evaluator path
            return super.executeMapping(testCase, testCaseIdentifier);
        }

        // SQL test case: detect database, load SQL, substitute DSN in mapping
        String sql;
        try {
            sql = new String(sqlStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to read resource.sql for test case %s".formatted(testCaseIdentifier), e);
        }

        var mappingBytes = readMappingBytes(testCase, testCaseIdentifier);
        var mappingContent = new String(mappingBytes, StandardCharsets.UTF_8);

        String jdbcUrl;
        String password = "test";
        if (mappingContent.contains(MYSQL_DRIVER)) {
            loadMySqlResourceSql(sql, testCaseIdentifier);
            jdbcUrl = MYSQL.getJdbcUrl();
        } else if (mappingContent.contains(POSTGRESQL_DRIVER)) {
            loadPostgreSqlResourceSql(sql, testCaseIdentifier);
            jdbcUrl = POSTGRESQL.getJdbcUrl();
        } else {
            throw new IllegalStateException(
                    "resource.sql found but mapping does not reference a known JDBC driver for test case %s"
                            .formatted(testCaseIdentifier));
        }

        // Substitute CONNECTIONDSN and password in the mapping so the DatabaseSource model has
        // the real JDBC URL. The DuckDbDatabaseAttacher parses this DSN to ATTACH the database.
        var substituted = mappingContent
                .replace("CONNECTIONDSN", jdbcUrl)
                .replace("d2rq:password \"\"", "d2rq:password \"%s\"".formatted(password));

        // Drop source cache tables from previous test case to prevent stale data collisions
        dropSourceCacheTables();

        var factory = new DuckDbLogicalViewEvaluatorFactory(getConnection());

        // Refresh the attachment for this DSN so DuckDB picks up the new tables. The attacher
        // needs to see the DatabaseSource from the parsed mapping to know which DSN to refresh.
        // We load the mapping, find the DatabaseSource, and refresh before running the mapper.
        Set<TriplesMap> mapping = RmlMappingLoader.build()
                .load(RDFFormat.TURTLE, new ByteArrayInputStream(substituted.getBytes(StandardCharsets.UTF_8)));
        refreshDatabaseAttachments(factory, mapping);

        var mapper = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(factory)
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), DuckDbTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    /**
     * Refreshes all database ATTACHments referenced by the mapping's database sources. This ensures
     * DuckDB's scanner sees the latest schema after SQL data is loaded into the test containers.
     * Walks through TriplesMap -> LogicalSource/LogicalView -> LogicalSource -> DatabaseSource.
     */
    private static void refreshDatabaseAttachments(DuckDbLogicalViewEvaluatorFactory factory, Set<TriplesMap> mapping) {
        var attacher = factory.getDatabaseAttacher();
        mapping.stream()
                .map(TriplesMap::getLogicalSource)
                .flatMap(als -> {
                    if (als instanceof io.carml.model.LogicalView view) {
                        var viewOn = view.getViewOn();
                        if (viewOn instanceof io.carml.model.LogicalSource ls) {
                            return java.util.stream.Stream.of(ls);
                        }
                        return java.util.stream.Stream.empty();
                    }
                    if (als instanceof io.carml.model.LogicalSource ls) {
                        return java.util.stream.Stream.of(ls);
                    }
                    return java.util.stream.Stream.empty();
                })
                .map(io.carml.model.LogicalSource::getSource)
                .filter(DatabaseSource.class::isInstance)
                .map(DatabaseSource.class::cast)
                .filter(ds -> ds.getJdbcDsn() != null && !ds.getJdbcDsn().isBlank())
                .distinct()
                .forEach(attacher::refresh);
    }

    private byte[] readMappingBytes(TestCase testCase, String testCaseIdentifier) {
        try {
            return getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument())
                    .readAllBytes();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read mapping for test case %s".formatted(testCaseIdentifier), e);
        }
    }

    // --- MySQL ---

    private void loadMySqlResourceSql(String sql, String testCaseIdentifier) {
        try (var conn = DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
                Statement stmt = conn.createStatement()) {
            for (String statement : sql.split(";")) {
                var trimmed = statement.trim();
                if (!trimmed.isEmpty()) {
                    stmt.execute(trimmed);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to execute resource.sql for MySQL test case %s".formatted(testCaseIdentifier), e);
        }
    }

    // --- PostgreSQL ---

    private void loadPostgreSqlResourceSql(String sql, String testCaseIdentifier) {
        try (var conn = DriverManager.getConnection(POSTGRESQL.getJdbcUrl(), "postgres", "test");
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to execute resource.sql for PostgreSQL test case %s".formatted(testCaseIdentifier), e);
        }
    }
}
