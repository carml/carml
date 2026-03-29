package io.carml.logicalview.duckdb;

import io.carml.model.DatabaseSource;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages the lifecycle of DuckDB database scanner ATTACHments derived from RML
 * {@link DatabaseSource} definitions. When a SQL logical source references an external database via a
 * JDBC DSN, this class installs and loads the required DuckDB extension, ATTACHes the database under
 * a deterministic alias, and returns the {@link CatalogSchema} needed to fully qualify table
 * references.
 *
 * <p>Fully qualified table names ({@code "<catalog>"."<schema>"."<table>"}) eliminate the need for
 * {@code USE <catalog>.<schema>} on duplicated connections, which do not inherit connection-level
 * settings from the base connection.
 *
 * <p>Each DSN is ATTACHed at most once per attacher instance. Re-attaching (DETACH + ATTACH) can be
 * triggered via {@link #refresh(DatabaseSource)} when the remote schema changes between invocations
 * (e.g., between test cases that DROP and re-CREATE tables).
 *
 * <p>Supported databases:
 * <ul>
 *   <li>MySQL ({@code com.mysql.cj.jdbc.Driver}, {@code com.mysql.jdbc.Driver})</li>
 *   <li>PostgreSQL ({@code org.postgresql.Driver})</li>
 * </ul>
 */
@Slf4j
public final class DuckDbDatabaseAttacher {

    /**
     * Pairs the DuckDB catalog alias with the schema name for fully qualifying table references.
     *
     * @param catalog the DuckDB catalog alias under which the database is ATTACHed
     * @param schema the default schema name within that catalog
     */
    public record CatalogSchema(String catalog, String schema) {}

    private static final String MYSQL_EXTENSION = "mysql";
    private static final String POSTGRES_EXTENSION = "postgres";

    private static final Map<String, String> DRIVER_TO_EXTENSION = Map.of(
            "com.mysql.cj.jdbc.Driver", MYSQL_EXTENSION,
            "com.mysql.jdbc.Driver", MYSQL_EXTENSION,
            "org.postgresql.Driver", POSTGRES_EXTENSION);

    private static final Map<String, String> DRIVER_TO_ATTACH_TYPE = Map.of(
            "com.mysql.cj.jdbc.Driver", "MYSQL",
            "com.mysql.jdbc.Driver", "MYSQL",
            "org.postgresql.Driver", "POSTGRES");

    private final Connection connection;

    /** Caches DSN to CatalogSchema to avoid redundant ATTACH calls. */
    private final ConcurrentHashMap<String, CatalogSchema> attached = new ConcurrentHashMap<>();

    DuckDbDatabaseAttacher(Connection connection) {
        this.connection = connection;
    }

    /**
     * ATTACHes the database described by the given {@link DatabaseSource} if it has not already been
     * ATTACHed. Returns the catalog alias and schema name for fully qualifying table references.
     *
     * @param dbSource the database source from the RML mapping
     * @return the catalog and schema for qualifying table names
     * @throws IllegalArgumentException if the JDBC driver is unsupported or the DSN cannot be parsed
     * @throws IllegalStateException if the ATTACH SQL execution fails
     */
    public CatalogSchema attachIfNeeded(DatabaseSource dbSource) {
        var dsn = dbSource.getJdbcDsn();
        if (dsn == null || dsn.isBlank()) {
            throw new IllegalArgumentException("DatabaseSource has no JDBC DSN");
        }

        return attached.computeIfAbsent(dsn, key -> doAttach(dbSource));
    }

    /**
     * Forces a re-ATTACH for the given database source by DETACHing and re-ATTACHing. This is needed
     * when the remote database schema has changed (e.g., tables were DROPped and re-CREATEd between
     * test cases) because DuckDB's scanner extensions may cache schema metadata.
     *
     * @param dbSource the database source to refresh
     * @return the (potentially updated) catalog and schema
     */
    public CatalogSchema refresh(DatabaseSource dbSource) {
        var dsn = dbSource.getJdbcDsn();
        var existing = attached.remove(dsn);
        if (existing != null) {
            doDetach(existing.catalog());
        }
        return attachIfNeeded(dbSource);
    }

    /**
     * DETACHes all previously ATTACHed databases. Called during factory cleanup.
     */
    public void detachAll() {
        for (var entry : attached.entrySet()) {
            doDetach(entry.getValue().catalog());
        }
        attached.clear();
    }

    private CatalogSchema doAttach(DatabaseSource dbSource) {
        var driver = dbSource.getJdbcDriver();
        var extension = DRIVER_TO_EXTENSION.get(driver);
        if (extension == null) {
            throw new IllegalArgumentException("Unsupported JDBC driver for DuckDB ATTACH: %s".formatted(driver));
        }

        var attachType = DRIVER_TO_ATTACH_TYPE.get(driver);
        var parsed = parseJdbcDsn(dbSource.getJdbcDsn());
        var alias = generateAlias(parsed);
        var attachString = buildAttachString(extension, parsed, dbSource);
        var schema = resolveSchema(extension, parsed);

        try {
            // DuckDB remembers installed extensions, so INSTALL is idempotent.
            // LOAD is needed per-connection but is also idempotent.
            try (var stmt = connection.createStatement()) {
                stmt.execute("INSTALL %s".formatted(extension));
                stmt.execute("LOAD %s".formatted(extension));
            }

            // DETACH any lingering attachment from previous factory instances sharing the same
            // base connection (e.g., in test scenarios). Uses a separate statement because
            // DuckDB's JDBC driver invalidates the statement after a failed execute.
            doDetach(alias);

            var attachSql = "ATTACH '%s' AS \"%s\" (TYPE %s)"
                    .formatted(attachString.replace("'", "''"), alias.replace("\"", "\"\""), attachType);
            LOG.debug("ATTACH SQL: {}", attachSql);
            try (var stmt = connection.createStatement()) {
                stmt.execute(attachSql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to ATTACH database from DSN: %s".formatted(dbSource.getJdbcDsn()), e);
        }

        var catalogSchema = new CatalogSchema(alias, schema);
        LOG.info("ATTACHed database as \"{}\" (TYPE {}) for DSN: {}", alias, attachType, dbSource.getJdbcDsn());
        return catalogSchema;
    }

    private void doDetach(String alias) {
        try (var stmt = connection.createStatement()) {
            stmt.execute("DETACH \"%s\"".formatted(alias.replace("\"", "\"\"")));
            LOG.debug("DETACHed database: {}", alias);
        } catch (SQLException e) {
            LOG.debug("DETACH '{}' skipped (not attached or already detached): {}", alias, e.getMessage());
        }
    }

    /**
     * Generates a deterministic catalog alias from the JDBC DSN. Uses a sanitized form of the DSN's
     * host, port, and database name to produce a human-readable alias.
     */
    static String generateAlias(String jdbcDsn) {
        return generateAlias(parseJdbcDsn(jdbcDsn));
    }

    static String generateAlias(ParsedDsn parsed) {
        var raw = "%s_%d_%s".formatted(parsed.host(), parsed.port(), parsed.database());
        var sanitized = raw.replaceAll("\\W", "_");
        return "__carml_%s".formatted(sanitized);
    }

    /**
     * Builds the DuckDB ATTACH connection string from the parsed DSN and database source credentials.
     *
     * <p>MySQL format: {@code host=<host> user=<user> password=<pass> port=<port> database=<db>}
     * <p>PostgreSQL format: {@code host=<host> user=<user> password=<pass> port=<port> dbname=<db>}
     */
    private static String buildAttachString(String extension, ParsedDsn parsed, DatabaseSource dbSource) {
        var user = dbSource.getUsername() != null ? dbSource.getUsername() : "";
        var pass = dbSource.getPassword() != null ? dbSource.getPassword().toString() : "";

        // Validate that values don't contain whitespace, which could inject additional key-value
        // pairs into the DuckDB ATTACH connection string (e.g., "pass host=evil" would override host).
        validateAttachValue("host", parsed.host());
        validateAttachValue("user", user);
        validateAttachValue("password", pass);
        validateAttachValue("database", parsed.database());

        var dbParam = MYSQL_EXTENSION.equals(extension) ? "database" : "dbname";
        return "host=%s user=%s password=%s port=%d %s=%s"
                .formatted(parsed.host(), user, pass, parsed.port(), dbParam, parsed.database());
    }

    private static void validateAttachValue(String key, String value) {
        if (value.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException(
                    "ATTACH connection string value for '%s' contains whitespace, which is not supported"
                            .formatted(key));
        }
    }

    /**
     * Resolves the default schema name for the ATTACHed database. MySQL schemas map to the database
     * name. PostgreSQL defaults to {@code public}.
     */
    private static String resolveSchema(String extension, ParsedDsn parsed) {
        if (MYSQL_EXTENSION.equals(extension)) {
            return parsed.database();
        }
        // PostgreSQL defaults to "public", others to "main"
        return POSTGRES_EXTENSION.equals(extension) ? "public" : "main";
    }

    /**
     * Parses a JDBC DSN to extract host, port, and database name.
     *
     * <p>Supported formats:
     * <ul>
     *   <li>{@code jdbc:mysql://host:port/database}</li>
     *   <li>{@code jdbc:postgresql://host:port/database}</li>
     * </ul>
     *
     * @param jdbcDsn the JDBC connection string
     * @return the parsed components
     * @throws IllegalArgumentException if the DSN format is not recognized
     */
    static ParsedDsn parseJdbcDsn(String jdbcDsn) {
        if (!jdbcDsn.startsWith("jdbc:")) {
            throw new IllegalArgumentException("Invalid JDBC DSN (missing 'jdbc:' prefix): %s".formatted(jdbcDsn));
        }

        var uriPart = jdbcDsn.substring(5); // Remove "jdbc:"
        try {
            var uri = URI.create(uriPart);
            var host = uri.getHost();
            var port = resolvePort(uri.getPort(), uriPart);
            var database = extractDatabase(uri.getPath(), jdbcDsn);

            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("Could not parse host from JDBC DSN: %s".formatted(jdbcDsn));
            }

            return new ParsedDsn(host, port, database);
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null
                    && (e.getMessage().startsWith("Could not parse")
                            || e.getMessage().startsWith("Invalid JDBC"))) {
                throw e;
            }
            throw new IllegalArgumentException("Failed to parse JDBC DSN: %s".formatted(jdbcDsn), e);
        }
    }

    private static String extractDatabase(String path, String jdbcDsn) {
        if (path == null || path.length() <= 1) {
            throw new IllegalArgumentException("Could not parse database name from JDBC DSN: %s".formatted(jdbcDsn));
        }

        // Remove leading '/' and strip query parameters (e.g., ?useSSL=false)
        var database = path.substring(1);
        var queryIdx = database.indexOf('?');
        return queryIdx >= 0 ? database.substring(0, queryIdx) : database;
    }

    private static int resolvePort(int port, String uriPart) {
        if (port >= 0) {
            return port;
        }
        if (uriPart.startsWith("mysql:")) {
            return 3306;
        }
        if (uriPart.startsWith("postgresql:")) {
            return 5432;
        }
        return port;
    }

    /**
     * Components extracted from a JDBC DSN.
     *
     * @param host the database server hostname
     * @param port the database server port
     * @param database the database name
     */
    record ParsedDsn(String host, int port, String database) {}
}
