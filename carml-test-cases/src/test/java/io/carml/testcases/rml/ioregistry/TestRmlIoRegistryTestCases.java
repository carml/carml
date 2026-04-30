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

    /**
     * Skip list for the rml-io-registry conformance suite running through the reactive SQL
     * evaluator (Vert.x MySQL / PostgreSQL clients), audited fresh against the upstream test cases
     * as of the 2026-04-20 sync. Tests are grouped by the underlying root cause; within each group,
     * each ID is a separate fixture exhibiting the same failure mode for the same reason.
     */
    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // ====================================================================
                // SQL Server tests — CARML's reactive SQL evaluator registers only
                // MySQL and PostgreSQL Vert.x client providers; the mapping declares
                // `d2rq:jdbcDriver "com.microsoft.sqlserver.jdbc.SQLServerDriver"`,
                // which {@link #executeMapping} cannot route, so it raises:
                //   IllegalStateException: resource.sql found but mapping does not
                //   reference a known JDBC driver for test case <id>
                // Unblocking these requires a SQL Server reactive client provider
                // (no Vert.x SQL Server client at present) plus a Testcontainers
                // mssql-server image in the suite setup.
                // ====================================================================
                "RMLIOREGTC0006a",
                "RMLIOREGTC0006c",
                "RMLIOREGTC0006f",
                "RMLIOREGTC0006k",
                "RMLIOREGTC0006l",
                "RMLIOREGTC0006n",
                "RMLIOREGTC0006p",
                "RMLIOREGTC0006q",
                "RMLIOREGTC0006r",
                "RMLIOREGTC0006s",
                "RMLIOREGTC0006t",
                "RMLIOREGTC0006u",
                "RMLIOREGTC0006v",
                "RMLIOREGTC0006w",
                "RMLIOREGTC0006x",
                "RMLIOREGTC0006y",
                "RMLIOREGTC0006z",

                // ====================================================================
                // HTTP-fetching sources require Netty's
                // `io.netty.channel.MultiThreadIoEventLoopGroup`, introduced in
                // Netty 4.2; the project pins an older Netty on the classpath, so
                // the HTTP source handler trips a NoClassDefFoundError before any
                // bytes are fetched. Affects DCAT and CSVW HTTP source variants.
                // ====================================================================
                "RMLIOREGTC0007a", // DCAT (dcat:Distribution + dcat:downloadURL)
                "RMLIOREGTC0012a", // CSVW (csvw:Table + csvw:url)

                // ====================================================================
                // Source vocabularies CARML's RDF mapper does not bind to a Java
                // type. The annotation-driven mapper raises:
                //   CarmlMapperException: could not find a java type corresponding
                //   to rdf type [<source-iri>]
                // ====================================================================
                "RMLIOREGTC0008a", // Web of Things — td:Thing
                "RMLIOREGTC0011a", // SPARQL endpoint — sd:Service

                // ====================================================================
                // Test-fixture bugs — invalid Turtle syntax. The mapping uses `;`
                // instead of `.` to terminate `@prefix` directives (lines 3-5),
                // which is not legal Turtle. The mapping never parses:
                //   RDFParseException: Expected '.', found ';' [line 3]
                // ====================================================================
                "RMLIOREGTC0009a", // Kafka source via WoT
                "RMLIOREGTC0010a", // MQTT source via WoT

                // ====================================================================
                // Test-fixture bugs — `assertThrows` expectations that do not match
                // RML semantics. The fixture declares "Error expected? Yes" for a
                // missing reference / null value, but the RML spec says missing
                // values produce no triple, not an error. CARML behaves correctly,
                // so the assertion fails:
                //   AssertionFailedError: Expected RuntimeException to be thrown,
                //   but nothing was thrown
                // ====================================================================
                "RMLIOREGTC0002b", // Missing JSON value
                "RMLIOREGTC0003b", // Missing XML value
                "RMLIOREGTC0005d", // PostgreSQL row with NULL DateOfBirth

                // ====================================================================
                // Test-fixture bug — namespace URI mismatch. The XML document
                // declares `xmlns="http://example.org/"` (trailing slash), but the
                // mapping's `rml:namespaceURL` is `"http://example.org"` (no
                // slash). XPath does not match any elements; the expected
                // `<http://example.com/Venus>` triple is missing.
                // ====================================================================
                "RMLIOREGTC0003d",

                // ====================================================================
                // Test-fixture bug — references a non-existent column in the SQL
                // SELECT list. The mapping's `rml:iterator` is
                // `SELECT NoColumnName, ID, Name FROM student`, but the schema only
                // has `ID` and `Name`; MySQL rejects with "Unknown column", which
                // surfaces as:
                //   RuntimeException: Failed to execute reactive SQL query
                // ====================================================================
                "RMLIOREGTC0004l",

                // ====================================================================
                // Test-fixture bugs — PostgreSQL column-name case-sensitivity.
                // Schemas declare double-quoted identifiers `"ID"`, `"Name"`, which
                // PostgreSQL stores case-sensitively, but the mappings' `rml:iterator`
                // uses unquoted `SELECT ID, Name FROM student`, which PostgreSQL
                // folds to lower case → "column id does not exist". Surfaces as:
                //   RuntimeException: Failed to execute reactive SQL query
                // ====================================================================
                "RMLIOREGTC0005k",
                "RMLIOREGTC0005l",

                // ====================================================================
                // Test-fixture bug — typed-vs-plain integer in expected output. The
                // SQL column is `INTEGER`, which RML-IO maps to `xsd:integer`, and
                // CARML correctly emits `"10"^^xsd:integer`. The fixture's
                // expected output writes a plain literal `"10"`.
                // ====================================================================
                "RMLIOREGTC0004k",

                // ====================================================================
                // xsd:double canonical-form mismatch. CARML's
                // {@code ValidatingValueFactory} normalises xsd:double to its W3C
                // XSD canonical form (`"3.0E1"`), which is the spec-correct
                // representation. The fixtures expect the non-canonical lexical
                // form (`"30.0"`).
                // ====================================================================
                "RMLIOREGTC0004o",
                "RMLIOREGTC0004t",
                "RMLIOREGTC0004w",
                "RMLIOREGTC0005o",
                "RMLIOREGTC0005t",
                "RMLIOREGTC0005w",

                // ====================================================================
                // MySQL BOOLEAN wire-protocol — MySQL stores `BOOLEAN` as
                // `TINYINT(1)`, and the Vert.x MySQL client surfaces it as `Byte`.
                // CARML emits `"0"^^xsd:byte` / `"1"^^xsd:byte` instead of the
                // expected `"false"^^xsd:boolean` / `"true"^^xsd:boolean`. Fixing
                // requires explicit boolean detection in the MySQL client provider.
                // ====================================================================
                "RMLIOREGTC0004y",

                // ====================================================================
                // Binary-column encoding — MySQL VARBINARY / PostgreSQL BYTEA
                // values arrive as Vert.x `Buffer` objects; CARML's term generator
                // currently URL-encodes raw bytes into the data: IRI rather than
                // hex-encoding them, producing
                //   data:image/png;hex,%FFFD...
                // when the fixture expects
                //   data:image/png;hex,89504E47...
                // (the 8-byte PNG signature). Needs a dedicated binary→hex path.
                // ====================================================================
                "RMLIOREGTC0004z",
                "RMLIOREGTC0005z",

                // ====================================================================
                // CSV column reference case-sensitivity. The fixture's `Friends.csv`
                // header is `id,name,age` but the mapping references `"ID"`. CARML's
                // CSV resolver treats column names case-sensitively, so the lookup
                // fails:
                //   NoSuchElementException: Header does not contain a field 'ID'.
                //   Valid names are: [id, name, age]
                // RML-IO doesn't mandate case-insensitive matching for CSV columns;
                // the fixture appears to assume it.
                // ====================================================================
                "RMLIOREGTC0012i");
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
