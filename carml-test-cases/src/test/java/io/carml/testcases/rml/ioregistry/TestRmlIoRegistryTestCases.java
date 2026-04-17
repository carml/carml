package io.carml.testcases.rml.ioregistry;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalview.DefaultLogicalViewEvaluatorFactory;
import io.carml.logicalview.sql.ReactiveSqlLogicalViewEvaluatorFactory;
import io.carml.logicalview.sql.mysql.MySqlClientProvider;
import io.carml.logicalview.sql.postgresql.PostgreSqlClientProvider;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.RmlTestCaseSuite;
import io.carml.util.RmlMappingLoader;
import io.vertx.core.Vertx;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Runs all W3C RML IO-Registry conformance tests using the reactive SQL evaluator for SQL-based
 * test cases, and the default evaluator for non-SQL test cases (JSON, XML, CSV).
 *
 * <p>MySQL tests (RMLIOREGTC0004*) run against a real MySQL container via the Vert.x MySQL client,
 * and PostgreSQL tests (RMLIOREGTC0005*) run against a real PostgreSQL container via the Vert.x
 * PostgreSQL client.
 *
 * <p>For non-SQL test cases, the default {@link DefaultLogicalViewEvaluatorFactory} is used,
 * identical to the baseline {@link TestRmlIoRegistryTestCases}.
 */
@Slf4j
class TestRmlIoRegistryTestCases extends RmlTestCaseSuite {

    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("test")
            .withUsername("root")
            .withPassword("test");

    @Container
    private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("test")
            .withUsername("postgres")
            .withPassword("test");

    private Vertx vertx;

    private ReactiveSqlLogicalViewEvaluatorFactory reactiveSqlFactory;

    @BeforeAll
    void setUpReactiveSql() {
        vertx = Vertx.vertx();
        reactiveSqlFactory = new ReactiveSqlLogicalViewEvaluatorFactory(
                List.of(new MySqlClientProvider(), new PostgreSqlClientProvider()), vertx);
        LOG.info(
                "Reactive SQL evaluator ready — MySQL: {}:{}, PostgreSQL: {}:{}",
                MYSQL.getHost(),
                MYSQL.getMappedPort(3306),
                POSTGRESQL.getHost(),
                POSTGRESQL.getMappedPort(5432));
    }

    @AfterAll
    void tearDownReactiveSql() {
        if (reactiveSqlFactory != null) {
            reactiveSqlFactory.close();
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        }
    }

    @Override
    protected String getBasePath() {
        return "/rml/ioregistry/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // --- Test case bugs (same as baseline) ---

                // JSON: $.THIS_VALUE_DOES_NOT_EXIST -> NULL, not error. The JSONPath IO-Registry spec
                // (section "Generation of null values") states that selectors referring to non-existent
                // JSON names result in NULL.
                "RMLIOREGTC0002b",

                // XML: NON_EXISTING -> NULL, not error. The XPath IO-Registry spec (section "Handling
                // absence of values") states that non-existent XPath evaluates to NULL.
                "RMLIOREGTC0003b",

                // Test case bug: namespace URL mismatch ("http://example.org" vs XML's
                // "http://example.org/") and unprefixed names in iterator/references for elements in
                // the default namespace.
                "RMLIOREGTC0003d",

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

                // CSVW with HTTP URL source: triggers reactor-netty HTTP fetch which fails due to
                // Netty 4.2 MultiThreadIoEventLoopGroup not on classpath
                "RMLIOREGTC0012a",

                // CSVW quoteChar: case-sensitive column name mismatch (CSV has lowercase headers,
                // mapping references uppercase)
                "RMLIOREGTC0012i",

                // --- SQL Server: no reactive SQL Server provider ---
                "RMLIOREGTC0006",

                // --- MySQL reactive issues ---

                // Natural type inference: produces xsd:integer where W3C test expects plain literal
                "RMLIOREGTC0004k",

                // hasError=false but query references non-existent column, causing query failure
                "RMLIOREGTC0004l",

                // FLOAT values: Java Float.toString() produces scientific notation (3.0E1 vs 30.0)
                "RMLIOREGTC0004o",
                "RMLIOREGTC0004t",
                "RMLIOREGTC0004w",

                // MySQL BOOLEAN column: wire protocol reports BOOLEAN as TINYINT without (1) precision,
                // so the driver can't distinguish it from a regular TINYINT
                "RMLIOREGTC0004y",

                // VARBINARY: Vert.x Buffer to hex string encoding not supported
                "RMLIOREGTC0004z",

                // --- PostgreSQL reactive issues ---

                // Error test: expected error not thrown (mapping succeeds with empty result)
                "RMLIOREGTC0005d",

                // Natural type inference: produces xsd:integer where W3C test expects plain literal
                "RMLIOREGTC0005k",

                // hasError=false but query references non-existent column, causing query failure
                "RMLIOREGTC0005l",

                // FLOAT values: Java Float.toString() produces scientific notation (3.0E1 vs 30.0)
                "RMLIOREGTC0005o",
                "RMLIOREGTC0005t",
                "RMLIOREGTC0005w",

                // BYTEA: Vert.x Buffer to hex string encoding not supported
                "RMLIOREGTC0005z");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var sqlStream = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, "resource.sql");
        if (sqlStream == null) {
            // Non-SQL test case: use default evaluator (same as baseline)
            return executeDefaultMapping(testCase, testCaseIdentifier);
        }

        // SQL test case: detect database, load SQL, use reactive evaluator
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

        // Substitute CONNECTIONDSN and password in the mapping
        var substituted = mappingContent
                .replace("CONNECTIONDSN", jdbcUrl)
                .replace("d2rq:password \"\"", "d2rq:password \"%s\"".formatted(password));
        var mapping = RmlMappingLoader.build()
                .load(RDFFormat.TURTLE, new ByteArrayInputStream(substituted.getBytes(StandardCharsets.UTF_8)));

        var mapper = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory())
                .logicalViewEvaluatorFactory(reactiveSqlFactory)
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    private Model executeDefaultMapping(TestCase testCase, String testCaseIdentifier) {
        var mappingBytes = readMappingBytes(testCase, testCaseIdentifier);
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, new ByteArrayInputStream(mappingBytes));

        var mapper = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory())
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
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
