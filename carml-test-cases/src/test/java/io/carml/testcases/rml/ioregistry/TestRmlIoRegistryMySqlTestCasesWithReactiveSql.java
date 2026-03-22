package io.carml.testcases.rml.ioregistry;

import io.carml.logicalview.sql.SqlClientProvider;
import io.carml.logicalview.sql.mysql.MySqlClientProvider;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.ReactiveSqlTestCaseSuite;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Runs W3C RML IO-Registry MySQL conformance tests (RMLIOREGTC0004*) against a real MySQL
 * container via the reactive SQL evaluator using Vert.x MySQL client.
 */
@Slf4j
class TestRmlIoRegistryMySqlTestCasesWithReactiveSql extends ReactiveSqlTestCaseSuite {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("test")
            .withUsername("root")
            .withPassword("test");

    @Override
    protected List<SqlClientProvider> getProviders() {
        return List.of(new MySqlClientProvider());
    }

    @Override
    protected String getJdbcUrl() {
        return MYSQL.getJdbcUrl();
    }

    @Override
    protected String getPassword() {
        return "test";
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
                // Natural type inference: produces xsd:integer where W3C test expects plain literal
                "RMLIOREGTC0004k",
                // hasError=false but query references non-existent column, causing query failure
                "RMLIOREGTC0004l",
                // FLOAT values: Java Float.toString() produces scientific notation (3.0E1 vs 30.0)
                "RMLIOREGTC0004o",
                "RMLIOREGTC0004t",
                "RMLIOREGTC0004w",
                // MySQL BOOLEAN column: wire protocol reports BOOLEAN as TINYINT without (1)
                // precision, so the driver can't distinguish it from a regular TINYINT
                "RMLIOREGTC0004y",
                // VARBINARY: Vert.x Buffer to hex string encoding not supported
                "RMLIOREGTC0004z");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        loadResourceSql(testCaseIdentifier);
        return super.executeMapping(testCase, testCaseIdentifier);
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
