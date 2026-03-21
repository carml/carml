package io.carml.testcases.rml.ioregistry;

import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Runs W3C RML IO-Registry MySQL conformance tests (RMLIOREGTC0004*) against a real MySQL
 * container via DuckDB's {@code mysql} scanner extension. For each test case, the
 * {@code resource.sql} is loaded into MySQL via JDBC, then the DuckDB scanner is refreshed via
 * DETACH/REATTACH to see the new tables.
 */
@Slf4j
class TestRmlIoRegistryMySqlTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("test")
            .withUsername("root")
            .withPassword("test");

    @BeforeAll
    void setUpMySqlScanner() throws SQLException {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("INSTALL mysql");
            stmt.execute("LOAD mysql");
            stmt.execute("ATTACH 'host=%s user=root password=test port=%d database=test' AS mysql_db (TYPE MYSQL)"
                    .formatted(MYSQL.getHost(), MYSQL.getMappedPort(3306)));
            stmt.execute("USE mysql_db.test");
        }
        LOG.info("DuckDB mysql scanner attached to {}:{}", MYSQL.getHost(), MYSQL.getMappedPort(3306));
    }

    @Override
    protected String getBasePath() {
        return "/rml/ioregistry/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // Non-MySQL tests
                "RMLIOREGTC0001",
                "RMLIOREGTC0002",
                "RMLIOREGTC0003",
                "RMLIOREGTC0005",
                "RMLIOREGTC0006",
                "RMLIOREGTC0007",
                "RMLIOREGTC0008",
                "RMLIOREGTC0009",
                "RMLIOREGTC0010",
                "RMLIOREGTC0011",
                "RMLIOREGTC0012",
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
                "RMLIOREGTC0004x");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        loadResourceSql(testCaseIdentifier);
        refreshMySqlAttachment();
        return super.executeMapping(testCase, testCaseIdentifier);
    }

    private void refreshMySqlAttachment() {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("USE memory");
            stmt.execute("DETACH mysql_db");
            stmt.execute("ATTACH 'host=%s user=root password=test port=%d database=test' AS mysql_db (TYPE MYSQL)"
                    .formatted(MYSQL.getHost(), MYSQL.getMappedPort(3306)));
            stmt.execute("USE mysql_db.test");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to refresh DuckDB mysql attachment", e);
        }
    }

    private void loadResourceSql(String testCaseIdentifier) {
        var sqlStream = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, "resource.sql");
        if (sqlStream == null) {
            throw new IllegalStateException("resource.sql not found for test case %s".formatted(testCaseIdentifier));
        }

        String sql;
        try {
            sql = new String(sqlStream.readAllBytes());
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to read resource.sql for test case %s".formatted(testCaseIdentifier), e);
        }

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
                    "Failed to execute resource.sql for test case %s".formatted(testCaseIdentifier), e);
        }
    }
}
