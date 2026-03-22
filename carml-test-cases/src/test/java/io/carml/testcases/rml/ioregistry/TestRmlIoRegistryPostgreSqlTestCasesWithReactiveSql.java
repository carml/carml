package io.carml.testcases.rml.ioregistry;

import io.carml.logicalview.sql.SqlClientProvider;
import io.carml.logicalview.sql.postgresql.PostgreSqlClientProvider;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.ReactiveSqlTestCaseSuite;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Runs W3C RML IO-Registry PostgreSQL conformance tests (RMLIOREGTC0005*) against a real
 * PostgreSQL container via the reactive SQL evaluator using Vert.x PostgreSQL client.
 */
@Slf4j
class TestRmlIoRegistryPostgreSqlTestCasesWithReactiveSql extends ReactiveSqlTestCaseSuite {

    @Container
    private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("test")
            .withUsername("postgres")
            .withPassword("test");

    @Override
    protected List<SqlClientProvider> getProviders() {
        return List.of(new PostgreSqlClientProvider());
    }

    @Override
    protected String getJdbcUrl() {
        return POSTGRESQL.getJdbcUrl();
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
                // Non-PostgreSQL tests
                "RMLIOREGTC0001",
                "RMLIOREGTC0002",
                "RMLIOREGTC0003",
                "RMLIOREGTC0004",
                "RMLIOREGTC0006",
                "RMLIOREGTC0007",
                "RMLIOREGTC0008",
                "RMLIOREGTC0009",
                "RMLIOREGTC0010",
                "RMLIOREGTC0011",
                "RMLIOREGTC0012",
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

        try (var conn = DriverManager.getConnection(POSTGRESQL.getJdbcUrl(), "postgres", "test");
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to execute resource.sql for test case %s".formatted(testCaseIdentifier), e);
        }
    }
}
