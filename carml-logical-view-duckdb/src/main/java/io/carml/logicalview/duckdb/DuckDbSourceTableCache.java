package io.carml.logicalview.duckdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * Caches DuckDB source SQL expressions as regular tables, ensuring each unique source is read only
 * once. When two logical views share the same source (e.g., the same {@code read_json_auto('file.json')}
 * call), the first encounter materializes the data into a table and subsequent encounters reuse it.
 *
 * <p>The cache key is the compiled {@code sourceSql} string. If two views produce identical source
 * SQL, they share the same table. Different iterators or source files produce different SQL and
 * therefore different tables.
 *
 * <p>Tables are created as regular (non-temporary) tables so they are visible across all connections
 * sharing the same DuckDB database (including connections created via {@code duplicate()}). Table
 * names include a unique instance prefix to avoid collisions between concurrent factory instances.
 * {@link #clear(Connection)} must be called for explicit cleanup before the factory is closed.
 *
 * <p>Tables are qualified with the native catalog and schema of the DuckDB database (determined at
 * construction time) so they are correctly referenced even when the evaluator connection has been
 * switched to an attached external database via {@code USE <catalog>.<schema>}. For in-memory
 * databases the qualifier is {@code "memory"."main"}; for on-disk databases it is
 * {@code "<dbname>"."main"}.
 *
 * <p><strong>Thread safety:</strong> This class is thread-safe. Multiple evaluator threads may call
 * {@link #getOrCreateTable(String, Connection)} concurrently; table creation is internally
 * synchronized to prevent duplicate CREATE TABLE statements.
 */
@Slf4j
class DuckDbSourceTableCache {

    private static final String TABLE_PREFIX = "__carml_src_";

    /** Sentinel value stored in the cache to represent a failed materialization attempt. */
    private static final String FAILED_SENTINEL = "";

    private final String instancePrefix;

    /**
     * Fully qualified catalog.schema prefix for table creation, e.g. {@code "memory"."main"} or
     * {@code "carml"."main"}. Determined from the factory connection at construction time, before
     * any {@code USE} commands are applied.
     */
    private final String nativeQualifier;

    private final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();

    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Creates a cache that stores tables in the native catalog/schema of the given connection.
     * This constructor queries the connection for its current catalog and schema, so it must be
     * called before any {@code USE <catalog>.<schema>} commands are applied.
     *
     * @param connection the DuckDB JDBC connection (must be in its initial catalog/schema state)
     */
    DuckDbSourceTableCache(Connection connection) {
        this.instancePrefix = UUID.randomUUID().toString().substring(0, 8);
        this.nativeQualifier = resolveNativeQualifier(connection);
    }

    /** Package-private constructor for testing with a known qualifier. */
    DuckDbSourceTableCache(String nativeQualifier) {
        this.instancePrefix = UUID.randomUUID().toString().substring(0, 8);
        this.nativeQualifier = nativeQualifier;
    }

    /**
     * Returns the fully qualified reference to the given table name, using the native catalog and
     * schema. Used by the view compiler to produce SQL that resolves to the cached table regardless
     * of the connection's current catalog/schema.
     *
     * @param tableName the unqualified table name
     * @return the qualified reference, e.g. {@code "memory"."main"."__carml_src_abc12345_0"}
     */
    String qualify(String tableName) {
        return "%s.\"%s\"".formatted(nativeQualifier, tableName);
    }

    /**
     * Returns the table name for the given source SQL, creating it if necessary. On first encounter,
     * executes {@code CREATE TABLE IF NOT EXISTS <qualified-name> AS SELECT * FROM <sourceSql>} to
     * materialize the source data as a regular table visible across all connections.
     *
     * <p>If table creation fails (e.g., due to insufficient memory for large sources), returns
     * {@code null} to signal that the caller should use the raw source SQL directly. The failure is
     * recorded so that subsequent calls for the same source SQL return {@code null} immediately
     * without retrying.
     *
     * <p>This method is thread-safe. Concurrent calls for different source SQL strings proceed in
     * parallel, while concurrent calls for the same source SQL string are serialized to prevent
     * duplicate table creation.
     *
     * @param sourceSql the source SQL expression (e.g., {@code read_json_auto('file.json')})
     * @param connection the DuckDB JDBC connection
     * @return the table name (unquoted), or {@code null} if materialization failed
     */
    @SuppressWarnings({
        "java:S2077", // sourceSql is generated by DuckDbSourceHandler, not from user input
        "java:S3649" // same: no user-supplied input reaches this SQL
    })
    String getOrCreateTable(String sourceSql, Connection connection) {
        // Fast path: table already cached (either a valid name or a failure sentinel).
        // ConcurrentHashMap.containsKey + get is safe here because values are never removed
        // except in clear(), which is only called during factory shutdown.
        var cached = cache.get(sourceSql);
        if (cached != null) {
            if (FAILED_SENTINEL.equals(cached)) {
                return null;
            }
            LOG.debug("Reusing cached source table [{}] for: {}", cached, sourceSql);
            return cached;
        }

        // Slow path: synchronize to ensure only one thread creates the table for a given sourceSql.
        synchronized (this) {
            // Double-check after acquiring the lock
            var doubleCheck = cache.get(sourceSql);
            if (doubleCheck != null) {
                return FAILED_SENTINEL.equals(doubleCheck) ? null : doubleCheck;
            }

            var tableName = TABLE_PREFIX + instancePrefix + "_" + counter.getAndIncrement();
            var qualifiedName = qualify(tableName);
            LOG.debug("Creating source table [{}] for: {}", qualifiedName, sourceSql);

            try (var statement = connection.createStatement()) {
                // Qualify with native catalog.schema to ensure the table is created in the DuckDB
                // database's own catalog, not in an attached external database that may have been
                // activated via USE <catalog>.<schema>.
                statement.execute(
                        "CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s".formatted(qualifiedName, sourceSql));
            } catch (SQLException e) {
                LOG.warn(
                        "Failed to materialize source into table, falling back to direct source read: {}",
                        e.getMessage());
                cache.put(sourceSql, FAILED_SENTINEL);
                return null;
            }

            cache.put(sourceSql, tableName);
            return tableName;
        }
    }

    /**
     * Drops all cached tables from the connection and clears the cache. Called defensively before the
     * factory connection is closed.
     *
     * @param connection the DuckDB JDBC connection (must be the base factory connection, since
     *     regular tables are visible across all connections sharing the same database)
     */
    void clear(Connection connection) {
        for (var tableName : cache.values()) {
            if (FAILED_SENTINEL.equals(tableName)) {
                continue;
            }
            try (var statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS %s".formatted(qualify(tableName)));
            } catch (SQLException e) {
                LOG.warn("Failed to drop source table [{}]", tableName, e);
            }
        }
        cache.clear();
        counter.set(0);
    }

    /** Returns the number of cached source tables. */
    int size() {
        return cache.size();
    }

    private static String resolveNativeQualifier(Connection connection) {
        try (var stmt = connection.createStatement();
                var rs = stmt.executeQuery("SELECT current_catalog(), current_schema()")) {
            if (rs.next()) {
                var catalog = rs.getString(1);
                var schema = rs.getString(2);
                var qualifier = "\"%s\".\"%s\"".formatted(catalog.replace("\"", "\"\""), schema.replace("\"", "\"\""));
                LOG.debug("Resolved native qualifier: {}", qualifier);
                return qualifier;
            }
        } catch (SQLException e) {
            LOG.warn("Could not determine native catalog/schema, defaulting to memory.main", e);
        }
        return "\"memory\".\"main\"";
    }
}
