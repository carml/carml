package io.carml.logicalview.sql;

import io.carml.model.DatabaseSource;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import java.sql.JDBCType;
import java.util.Optional;
import org.eclipse.rdf4j.model.IRI;
import org.jooq.SQLDialect;

/**
 * SPI for database-specific Vert.x SQL client creation. Each supported database type (MySQL,
 * PostgreSQL, etc.) provides an implementation discovered via {@link java.util.ServiceLoader}.
 *
 * <p>Implementations supply:
 * <ul>
 *   <li>Database detection via JDBC driver class name</li>
 *   <li>Vert.x connection pool creation from {@link DatabaseSource} credentials</li>
 *   <li>jOOQ {@link SQLDialect} for SQL query construction</li>
 *   <li>SQL column type to XSD datatype mapping for natural datatype inference</li>
 * </ul>
 */
public interface SqlClientProvider {

    /**
     * Returns whether this provider supports the given database source, typically by inspecting the
     * JDBC driver class name.
     *
     * @param source the database source to check
     * @return {@code true} if this provider can create a pool for the source
     */
    boolean supports(DatabaseSource source);

    /**
     * Creates a Vert.x SQL connection pool for the given database source.
     *
     * @param vertx the Vert.x instance to use for the pool
     * @param source the database source containing connection parameters
     * @return a configured connection pool
     */
    Pool createPool(Vertx vertx, DatabaseSource source);

    /**
     * Returns the jOOQ SQL dialect for this database type.
     *
     * @return the SQL dialect
     */
    SQLDialect dialect();

    /**
     * Maps a SQL column type to its natural XSD datatype IRI using the JDBC type and
     * database-native type name from the result set column descriptor. This is the preferred mapping
     * method because it has access to the full column type information (e.g., MySQL
     * {@code TINYINT(1)} vs {@code TINYINT}, unsigned variants, etc.).
     *
     * <p>Returns empty for types that have no natural RDF datatype (e.g., {@code VARCHAR}).
     *
     * @param jdbcType the JDBC type from the column descriptor
     * @param typeName the database-native type name (e.g., {@code "TINYINT"}, {@code "BIGINT
     *     UNSIGNED"}, {@code "BOOL"})
     * @return an optional XSD datatype IRI
     */
    Optional<IRI> mapNaturalDatatype(JDBCType jdbcType, String typeName);

    /**
     * Converts a JDBC URL to a Vert.x connection URI by stripping the {@code jdbc:} prefix. Vert.x
     * SQL clients accept native connection URIs (e.g., {@code mysql://host:port/db},
     * {@code postgresql://host:port/db}) which share the same format as JDBC URLs minus the
     * {@code jdbc:} prefix.
     *
     * @param jdbcUrl the JDBC URL (e.g., {@code jdbc:mysql://host:3306/db})
     * @return the connection URI (e.g., {@code mysql://host:3306/db})
     * @throws IllegalArgumentException if the URL does not start with {@code jdbc:}
     */
    static String toConnectionUri(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:")) {
            throw new IllegalArgumentException("Not a JDBC URL: %s".formatted(jdbcUrl));
        }
        return jdbcUrl.substring(5);
    }
}
