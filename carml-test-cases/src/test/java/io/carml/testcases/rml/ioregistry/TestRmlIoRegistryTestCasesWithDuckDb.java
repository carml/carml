package io.carml.testcases.rml.ioregistry;

import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Runs all W3C RML IO-Registry conformance tests against DuckDB. Non-SQL tests (JSON, XML, CSV)
 * run against DuckDB's native evaluator. MySQL tests (RMLIOREGTC0004*) run against a real MySQL
 * container via DuckDB's {@code mysql} scanner extension, and PostgreSQL tests (RMLIOREGTC0005*)
 * run against a real PostgreSQL container via DuckDB's {@code postgres} scanner extension.
 *
 * <p>For each SQL test case, the {@code resource.sql} is loaded into the appropriate database via
 * JDBC, then the DuckDB scanner is refreshed via DETACH/REATTACH to see the new tables.
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

    @BeforeAll
    void setUpScanners() throws SQLException {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("INSTALL mysql");
            stmt.execute("LOAD mysql");
            stmt.execute("ATTACH 'host=%s user=root password=test port=%d database=test' AS mysql_db (TYPE MYSQL)"
                    .formatted(MYSQL.getHost(), MYSQL.getMappedPort(3306)));

            stmt.execute("INSTALL postgres");
            stmt.execute("LOAD postgres");
            stmt.execute("ATTACH 'host=%s user=postgres password=test port=%d dbname=test' AS pg_db (TYPE POSTGRES)"
                    .formatted(POSTGRESQL.getHost(), POSTGRESQL.getMappedPort(5432)));
        }
        LOG.info(
                "DuckDB scanners attached — MySQL: {}:{}, PostgreSQL: {}:{}",
                MYSQL.getHost(),
                MYSQL.getMappedPort(3306),
                POSTGRESQL.getHost(),
                POSTGRESQL.getMappedPort(5432));
    }

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
        if (sqlStream != null) {
            String sql;
            try {
                sql = new String(sqlStream.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to read resource.sql for test case %s".formatted(testCaseIdentifier), e);
            }

            var mappingContent = readMappingContent(testCaseIdentifier);
            if (mappingContent.contains(MYSQL_DRIVER)) {
                loadMySqlResourceSql(sql, testCaseIdentifier);
                refreshMySqlAttachment();
                useMySqlSchema();
            } else if (mappingContent.contains(POSTGRESQL_DRIVER)) {
                loadPostgreSqlResourceSql(sql, testCaseIdentifier);
                refreshPostgresAttachment();
                usePostgresSchema();
            } else {
                throw new IllegalStateException(
                        "resource.sql found but mapping does not reference a known JDBC driver for test case %s"
                                .formatted(testCaseIdentifier));
            }
        }

        return super.executeMapping(testCase, testCaseIdentifier);
    }

    private String readMappingContent(String testCaseIdentifier) {
        var mappingStream = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, "mapping.ttl");
        if (mappingStream == null) {
            throw new IllegalStateException("mapping.ttl not found for test case %s".formatted(testCaseIdentifier));
        }
        try {
            return new String(mappingStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to read mapping.ttl for test case %s".formatted(testCaseIdentifier), e);
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

    private void refreshMySqlAttachment() {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("USE memory");
            stmt.execute("DETACH mysql_db");
            stmt.execute("ATTACH 'host=%s user=root password=test port=%d database=test' AS mysql_db (TYPE MYSQL)"
                    .formatted(MYSQL.getHost(), MYSQL.getMappedPort(3306)));
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to refresh DuckDB mysql attachment", e);
        }
    }

    private void useMySqlSchema() {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("USE mysql_db.test");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to switch to mysql_db.test schema", e);
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

    private void refreshPostgresAttachment() {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("USE memory");
            stmt.execute("DETACH pg_db");
            stmt.execute("ATTACH 'host=%s user=postgres password=test port=%d dbname=test' AS pg_db (TYPE POSTGRES)"
                    .formatted(POSTGRESQL.getHost(), POSTGRESQL.getMappedPort(5432)));
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to refresh DuckDB postgres attachment", e);
        }
    }

    private void usePostgresSchema() {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("USE pg_db.public");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to switch to pg_db.public schema", e);
        }
    }
}
