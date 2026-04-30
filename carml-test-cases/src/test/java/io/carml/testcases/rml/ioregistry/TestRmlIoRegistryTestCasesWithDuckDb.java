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
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
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

    /**
     * Skip list for the rml-io-registry conformance suite running through the DuckDB evaluator,
     * audited fresh against the upstream test cases as of the 2026-04-20 sync. The set differs from
     * {@link TestRmlIoRegistryTestCases} (the reactive variant) primarily because:
     *
     * <ul>
     *   <li>DuckDB has no XML / XPath support, so XML test cases that pass on the reactive
     *       evaluator fail here with "No evaluator matched logical view".
     *   <li>DuckDB's dialect differs from MySQL/PostgreSQL — some `rml:iterator` SQL strings that
     *       run on the reactive Vert.x clients fail in DuckDB's scanner-extension dialect.
     *   <li>DuckDB returns DATE/TIMESTAMP and BLOB Java values that don't slot directly into
     *       RDF4J's literal factory the way the Vert.x clients' typed values do.
     * </ul>
     */
    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // ====================================================================
                // SQL Server tests — DuckDB has no SQL Server scanner extension, and
                // {@link #executeMapping} only routes mappings whose `d2rq:jdbcDriver`
                // is MySQL or PostgreSQL. Mappings with
                // `com.microsoft.sqlserver.jdbc.SQLServerDriver` raise:
                //   IllegalStateException: resource.sql found but mapping does not
                //   reference a known JDBC driver for test case <id>
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
                // XML / XPath sources are not handled by the DuckDB evaluator (no
                // native XML scanner). The evaluator factory returns no match and
                // dispatch fails with:
                //   RmlMapperException: No evaluator matched logical view ...
                // The {@link RmlTestCaseSuite} single-evaluator harness has no
                // fallback to the reactive evaluator.
                // ====================================================================
                "RMLIOREGTC0003a", // XML basic
                "RMLIOREGTC0003d", // XML namespace
                "RMLIOREGTC0003e", // XML namespace + XPath functions

                // ====================================================================
                // DCAT-only HTTP source — the DuckDB evaluator factory does not
                // recognise `dcat:Distribution` + `rml:JSONPath` as a source it can
                // handle (no DCAT->scanner translation). Same RmlMapperException as
                // the XML cases above. (For the CSVW HTTP variant 0012a, DuckDB's
                // httpfs extension covers the fetch and the test passes.)
                // ====================================================================
                "RMLIOREGTC0007a",

                // ====================================================================
                // Source vocabularies CARML's RDF mapper does not bind to a Java
                // type — same as the reactive suite:
                //   CarmlMapperException: could not find a java type corresponding
                //   to rdf type [<source-iri>]
                // ====================================================================
                "RMLIOREGTC0008a", // td:Thing (Web of Things)
                "RMLIOREGTC0011a", // sd:Service (SPARQL endpoint)

                // ====================================================================
                // Test-fixture bugs — invalid Turtle (`@prefix` lines terminated
                // with `;` instead of `.`):
                //   RDFParseException: Expected '.', found ';' [line 3]
                // ====================================================================
                "RMLIOREGTC0009a", // Kafka source via WoT
                "RMLIOREGTC0010a", // MQTT source via WoT

                // ====================================================================
                // Test-fixture bug — `assertThrows` expectation that does not match
                // RML semantics. Missing JSON values produce no triple per spec, not
                // an error; CARML behaves correctly so the assertion fails:
                //   AssertionFailedError: Expected RuntimeException to be thrown,
                //   but nothing was thrown
                // (Note: the equivalent XML / PostgreSQL fixtures 0003b and 0005d
                // happen to error out earlier in the DuckDB code path, accidentally
                // satisfying assertThrows there — so they are NOT in this skip list
                // even though they are in the reactive one.)
                // ====================================================================
                "RMLIOREGTC0002b",

                // ====================================================================
                // DuckDB SQL-dialect incompatibilities. DuckDB's scanner extensions
                // pass the `rml:iterator` query through to the underlying engine,
                // but the source-table cache materialisation step also runs the
                // query against DuckDB itself, where MySQL/PostgreSQL syntax or
                // schema references that work on Vert.x fail. Surfaces as:
                //   DuckDbQueryException: Failed to execute DuckDB query for view ...
                // with a fallback warning from {@code DuckDbSourceTableCache}.
                // ====================================================================
                "RMLIOREGTC0004l", // SELECT references a non-existent column
                "RMLIOREGTC0004n", // multi-column concatenation query
                "RMLIOREGTC0005l", // SELECT references a non-existent column

                // ====================================================================
                // DATE / TIMESTAMP literal generation. DuckDB returns these as Java
                // `java.time.LocalDate` / `Timestamp` values that CARML's term
                // generator passes verbatim to {@code ValidatingValueFactory}, which
                // rejects them:
                //   IllegalArgumentException: Not a valid literal value
                // (RdfTermGeneratorFactory.lambda$generateDatatypedLiterals$16)
                // Needs an explicit DuckDB date/timestamp -> xsd:date/xsd:dateTime
                // converter in the source handler.
                // ====================================================================
                "RMLIOREGTC0004x",
                "RMLIOREGTC0005x",

                // ====================================================================
                // Test-fixture bug — typed-vs-plain integer in expected output.
                // Integer columns map to xsd:integer per RML-IO; CARML emits
                // `"10"^^xsd:integer`. The fixtures expect a plain literal `"10"`.
                // ====================================================================
                "RMLIOREGTC0004k",
                "RMLIOREGTC0005k",

                // ====================================================================
                // xsd:double canonical-form mismatch. CARML's
                // {@code ValidatingValueFactory} normalises xsd:double to its W3C
                // XSD canonical form (`"3.0E1"`); fixtures expect the non-canonical
                // form (`"30.0"`).
                // ====================================================================
                "RMLIOREGTC0004o",
                "RMLIOREGTC0004t",
                "RMLIOREGTC0004w",
                "RMLIOREGTC0005o",
                "RMLIOREGTC0005t",
                "RMLIOREGTC0005w",

                // ====================================================================
                // BLOB encoding — DuckDB returns BLOB columns as a Java `byte[]`.
                // CARML's term generator stringifies the array via Java's default
                // {@code Object.toString()}, producing
                //   data:image/png;hex,%5BB%40<hashcode>
                // (URL-encoded `[B@<hash>`) instead of the expected hex-encoded
                // bytes:
                //   data:image/png;hex,89504E47...
                // Needs a binary-aware path that hex-encodes byte arrays.
                // ====================================================================
                "RMLIOREGTC0004z",
                "RMLIOREGTC0005z",

                // ====================================================================
                // CSVW UTF-16 encoding — DuckDB's `read_csv` does not accept
                // `encoding='utf-16'` (only utf-8, latin-1, and a few CJK codepages
                // are supported in the bundled CSV reader). Surfaces as:
                //   DuckDbQueryException: Failed to execute DuckDB query for view ...
                // ====================================================================
                "RMLIOREGTC0012f",

                // ====================================================================
                // CSV column reference case-sensitivity. The fixture's CSV header
                // is `id,name,age` but the mapping references `"ID"`. The DuckDB
                // CSV source handler validates column references case-sensitively:
                //   IllegalArgumentException: CSV column reference 'ID' does not
                //   match any column header case-sensitively. Available columns:
                //   [id, name, age]
                // ====================================================================
                "RMLIOREGTC0012i");
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
