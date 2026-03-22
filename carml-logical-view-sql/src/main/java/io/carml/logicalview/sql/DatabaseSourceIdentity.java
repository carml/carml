package io.carml.logicalview.sql;

import io.carml.model.DatabaseSource;
import java.util.Objects;

/**
 * Value type identifying a unique database connection target. Two {@link DatabaseSource} instances
 * with the same identity point to the same database and can share a connection pool.
 *
 * @param jdbcDsn the JDBC connection string
 * @param jdbcDriver the JDBC driver class name
 * @param username the database username
 */
public record DatabaseSourceIdentity(String jdbcDsn, String jdbcDriver, String username) {

    /**
     * Extracts the database identity from a {@link DatabaseSource}.
     *
     * @param source the database source
     * @return the identity
     */
    public static DatabaseSourceIdentity of(DatabaseSource source) {
        Objects.requireNonNull(source, "source must not be null");
        return new DatabaseSourceIdentity(source.getJdbcDsn(), source.getJdbcDriver(), source.getUsername());
    }
}
