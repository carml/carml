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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Runs W3C RML IO-Registry PostgreSQL conformance tests (RMLIOREGTC0005*) against a real
 * PostgreSQL container via DuckDB's {@code postgres} scanner extension.
 */
@Slf4j
class TestRmlIoRegistryPostgreSqlTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Container
    private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("test")
            .withUsername("postgres")
            .withPassword("test");

    @BeforeAll
    void setUpPostgresScanner() throws SQLException {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("INSTALL postgres");
            stmt.execute("LOAD postgres");
            stmt.execute("ATTACH 'host=%s user=postgres password=test port=%d dbname=test' AS pg_db (TYPE POSTGRES)"
                    .formatted(POSTGRESQL.getHost(), POSTGRESQL.getMappedPort(5432)));
            stmt.execute("USE pg_db.public");
        }
        LOG.info("DuckDB postgres scanner attached to {}:{}", POSTGRESQL.getHost(), POSTGRESQL.getMappedPort(5432));
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
        loadResourceSql(testCaseIdentifier);
        refreshPostgresAttachment();
        return super.executeMapping(testCase, testCaseIdentifier);
    }

    private void refreshPostgresAttachment() {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute("USE memory");
            stmt.execute("DETACH pg_db");
            stmt.execute("ATTACH 'host=%s user=postgres password=test port=%d dbname=test' AS pg_db (TYPE POSTGRES)"
                    .formatted(POSTGRESQL.getHost(), POSTGRESQL.getMappedPort(5432)));
            stmt.execute("USE pg_db.public");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to refresh DuckDB postgres attachment", e);
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

        try (var conn = DriverManager.getConnection(POSTGRESQL.getJdbcUrl(), "postgres", "test");
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to execute resource.sql for test case %s".formatted(testCaseIdentifier), e);
        }
    }
}
