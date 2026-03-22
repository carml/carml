package io.carml.logicalview.sql.postgresql;

import com.google.auto.service.AutoService;
import io.carml.logicalview.sql.SqlClientProvider;
import io.carml.model.DatabaseSource;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.sql.JDBCType;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.jooq.SQLDialect;

/**
 * {@link SqlClientProvider} for PostgreSQL databases. Supports JDBC driver class name
 * {@code org.postgresql.Driver}.
 *
 * <p>Natural datatype mapping follows the PostgreSQL type system:
 * <ul>
 *   <li>SMALLINT, INTEGER, BIGINT, SERIAL, BIGSERIAL → xsd:integer</li>
 *   <li>NUMERIC, DECIMAL → xsd:decimal</li>
 *   <li>REAL, FLOAT, DOUBLE PRECISION → xsd:double</li>
 *   <li>BOOLEAN → xsd:boolean</li>
 *   <li>DATE → xsd:date</li>
 *   <li>TIME, TIME WITH TIME ZONE → xsd:time</li>
 *   <li>TIMESTAMP, TIMESTAMP WITH TIME ZONE → xsd:dateTime</li>
 *   <li>BYTEA → xsd:hexBinary</li>
 * </ul>
 */
@Slf4j
@AutoService(SqlClientProvider.class)
public class PostgreSqlClientProvider implements SqlClientProvider {

    private static final Set<String> SUPPORTED_DRIVERS = Set.of("org.postgresql.Driver");

    private static final int DEFAULT_POOL_SIZE = 4;

    @Override
    public boolean supports(DatabaseSource source) {
        return source.getJdbcDriver() != null && SUPPORTED_DRIVERS.contains(source.getJdbcDriver());
    }

    @Override
    public Pool createPool(Vertx vertx, DatabaseSource source) {
        return createPool(vertx, source, DEFAULT_POOL_SIZE);
    }

    @Override
    public Pool createPool(Vertx vertx, DatabaseSource source, int maxPoolSize) {
        var connectOptions = PgConnectOptions.fromUri(SqlClientProvider.toConnectionUri(source.getJdbcDsn()));

        if (source.getUsername() != null) {
            connectOptions.setUser(source.getUsername());
        }
        if (source.getPassword() != null) {
            connectOptions.setPassword(source.getPassword().toString());
        }

        LOG.debug(
                "Creating PostgreSQL pool (size={}) for {}:{}/{}",
                maxPoolSize,
                connectOptions.getHost(),
                connectOptions.getPort(),
                connectOptions.getDatabase());

        return Pool.pool(vertx, connectOptions, new PoolOptions().setMaxSize(maxPoolSize));
    }

    @Override
    public SQLDialect dialect() {
        return SQLDialect.POSTGRES;
    }

    @Override
    public Optional<IRI> mapNaturalDatatype(JDBCType jdbcType, String typeName) {
        return switch (jdbcType) {
            case TINYINT, SMALLINT, INTEGER, BIGINT -> Optional.of(XSD.INTEGER);
            case DECIMAL, NUMERIC -> Optional.of(XSD.DECIMAL);
            case FLOAT, REAL, DOUBLE -> Optional.of(XSD.DOUBLE);
            case BIT, BOOLEAN -> Optional.of(XSD.BOOLEAN);
            case DATE -> Optional.of(XSD.DATE);
            case TIME, TIME_WITH_TIMEZONE -> Optional.of(XSD.TIME);
            case TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> Optional.of(XSD.DATETIME);
            case BINARY, VARBINARY, LONGVARBINARY, BLOB -> Optional.of(XSD.HEXBINARY);
            default -> mapByTypeName(typeName);
        };
    }

    /**
     * Fallback mapping for PostgreSQL-specific types not covered by standard JDBC types.
     */
    private static Optional<IRI> mapByTypeName(String typeName) {
        if (typeName == null) {
            return Optional.empty();
        }
        var upper = typeName.toUpperCase(Locale.ROOT);
        if (upper.equals("SERIAL") || upper.equals("BIGSERIAL") || upper.equals("SMALLSERIAL")) {
            return Optional.of(XSD.INTEGER);
        }
        if (upper.equals("BYTEA")) {
            return Optional.of(XSD.HEXBINARY);
        }
        return Optional.empty();
    }
}
