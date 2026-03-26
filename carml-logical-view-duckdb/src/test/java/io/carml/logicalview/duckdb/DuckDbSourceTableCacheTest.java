package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DuckDbSourceTableCacheTest {

    private static Connection connection;

    private DuckDbSourceTableCache cache;

    @BeforeAll
    static void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @BeforeEach
    void beforeEach() {
        cache = new DuckDbSourceTableCache(connection);
    }

    @AfterEach
    void afterEach() {
        cache.clear(connection);
    }

    @Test
    void getOrCreate_sameSql_returnsSameTable() {
        var sourceSql = "(SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob')";

        var table1 = cache.getOrCreateTable(sourceSql, connection);
        var table2 = cache.getOrCreateTable(sourceSql, connection);

        assertThat(table1, is(table2));
        assertThat(cache.size(), is(1));
    }

    @Test
    void getOrCreate_differentSql_returnsDifferentTables() {
        var sql1 = "(SELECT 1 AS id)";
        var sql2 = "(SELECT 2 AS id)";

        var table1 = cache.getOrCreateTable(sql1, connection);
        var table2 = cache.getOrCreateTable(sql2, connection);

        assertThat(table1, is(not(table2)));
        assertThat(cache.size(), is(2));
    }

    @Test
    void getOrCreate_createsQueryableTable() throws SQLException {
        var sourceSql = "(SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob')";

        var tableName = cache.getOrCreateTable(sourceSql, connection);

        try (var statement = connection.createStatement();
                var resultSet = statement.executeQuery("SELECT count(*) FROM \"%s\"".formatted(tableName))) {
            resultSet.next();
            assertThat(resultSet.getInt(1), is(2));
        }
    }

    @Test
    void getOrCreate_invalidSql_returnsNullAndRecordsFailure() {
        var invalidSql = "this_is_not_valid_sql()";

        var result = cache.getOrCreateTable(invalidSql, connection);

        assertThat(result, is(nullValue()));
        assertThat(cache.size(), is(1));

        // Second call returns null immediately without retrying
        var result2 = cache.getOrCreateTable(invalidSql, connection);
        assertThat(result2, is(nullValue()));
    }

    @Test
    void clear_withNullEntry_completesWithoutException() {
        // Create a failed entry (null value in cache)
        cache.getOrCreateTable("this_is_not_valid_sql()", connection);
        assertThat(cache.size(), is(1));

        // clear() should complete without exception even with null entries
        cache.clear(connection);
        assertThat(cache.size(), is(0));
    }

    @Test
    void clear_resetsCounter_nextTableStartsAtZero() {
        var sql1 = "(SELECT 1 AS id)";
        var first = cache.getOrCreateTable(sql1, connection);
        assertThat(first, startsWith("__carml_src_"));
        // Table name ends with _0 (first counter value)
        assertThat(first.endsWith("_0"), is(true));

        cache.clear(connection);

        var sql2 = "(SELECT 2 AS id)";
        var afterClear = cache.getOrCreateTable(sql2, connection);
        // After clear, counter resets so the suffix is _0 again
        assertThat(afterClear.endsWith("_0"), is(true));
    }

    @Test
    void clear_dropsTablesFromConnection() throws SQLException {
        var sourceSql = "(SELECT 1 AS id)";
        var tableName = cache.getOrCreateTable(sourceSql, connection);

        cache.clear(connection);

        assertThat(cache.size(), is(0));

        // Verify the table no longer exists by querying the information schema
        try (var statement = connection.createStatement();
                var resultSet =
                        statement.executeQuery("SELECT count(*) FROM information_schema.tables WHERE table_name = '%s'"
                                .formatted(tableName))) {
            resultSet.next();
            assertThat(resultSet.getInt(1), is(0));
        }
    }

    @Test
    void qualify_inMemory_usesMemoryMain() {
        var qualified = cache.qualify("my_table");
        assertThat(qualified, is("\"memory\".\"main\".\"my_table\""));
    }

    @Nested
    class OnDiskMode {

        @Test
        void getOrCreate_onDiskConnection_createsQueryableTable() throws SQLException, IOException {
            var tempDir = Files.createTempDirectory("duckdb-cache-test-");
            var dbPath = tempDir.resolve("test.duckdb");
            try (var onDiskConn = DriverManager.getConnection("jdbc:duckdb:" + dbPath)) {
                var onDiskCache = new DuckDbSourceTableCache(onDiskConn);

                var sourceSql = "(SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob')";
                var tableName = onDiskCache.getOrCreateTable(sourceSql, onDiskConn);

                assertThat(tableName, is(not(nullValue())));

                // Verify the table is queryable using the qualified name
                var qualifiedName = onDiskCache.qualify(tableName);
                try (var stmt = onDiskConn.createStatement();
                        var rs = stmt.executeQuery("SELECT count(*) FROM %s".formatted(qualifiedName))) {
                    rs.next();
                    assertThat(rs.getInt(1), is(2));
                }

                onDiskCache.clear(onDiskConn);
            } finally {
                // Clean up temporary files
                try (var entries = Files.walk(tempDir)) {
                    entries.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                            // best effort
                        }
                    });
                }
            }
        }

        @Test
        void qualify_onDiskConnection_usesDatabaseCatalog() throws SQLException, IOException {
            var tempDir = Files.createTempDirectory("duckdb-cache-test-");
            var dbPath = tempDir.resolve("mydb.duckdb");
            try (var onDiskConn = DriverManager.getConnection("jdbc:duckdb:" + dbPath)) {
                var onDiskCache = new DuckDbSourceTableCache(onDiskConn);

                var qualified = onDiskCache.qualify("my_table");
                // On-disk DuckDB catalog name is derived from the database file stem
                assertThat(qualified, is("\"mydb\".\"main\".\"my_table\""));
            } finally {
                try (var entries = Files.walk(tempDir)) {
                    entries.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                            // best effort
                        }
                    });
                }
            }
        }
    }
}
