package io.carml.logicalview.sql.mysql;

import com.google.auto.service.AutoService;
import io.carml.logicalview.sql.SqlClientProvider;
import io.carml.model.DatabaseSource;
import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLConnectOptions;
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
 * {@link SqlClientProvider} for MySQL databases. Supports JDBC driver class names
 * {@code com.mysql.cj.jdbc.Driver} and {@code com.mysql.jdbc.Driver}.
 *
 * <p>Natural datatype mapping follows the MySQL type system:
 * <ul>
 *   <li>TINYINT(1) / BOOL → xsd:boolean</li>
 *   <li>TINYINT (non-boolean) → xsd:byte</li>
 *   <li>SMALLINT, MEDIUMINT, INT, BIGINT → xsd:integer</li>
 *   <li>TINYINT UNSIGNED → xsd:unsignedByte</li>
 *   <li>SMALLINT UNSIGNED → xsd:unsignedShort</li>
 *   <li>MEDIUMINT UNSIGNED, INT UNSIGNED → xsd:unsignedInt</li>
 *   <li>BIGINT UNSIGNED → xsd:unsignedLong</li>
 *   <li>DECIMAL → xsd:decimal</li>
 *   <li>FLOAT, DOUBLE → xsd:double</li>
 *   <li>DATE → xsd:date</li>
 *   <li>TIME → xsd:time</li>
 *   <li>YEAR → xsd:gYear</li>
 *   <li>TIMESTAMP, DATETIME → xsd:dateTime</li>
 *   <li>BINARY, VARBINARY, BLOB variants → xsd:hexBinary</li>
 * </ul>
 */
@Slf4j
@AutoService(SqlClientProvider.class)
public class MySqlClientProvider implements SqlClientProvider {

    private static final Set<String> SUPPORTED_DRIVERS = Set.of("com.mysql.cj.jdbc.Driver", "com.mysql.jdbc.Driver");

    private static final int DEFAULT_POOL_SIZE = 4;

    @Override
    public boolean supports(DatabaseSource source) {
        return source.getJdbcDriver() != null && SUPPORTED_DRIVERS.contains(source.getJdbcDriver());
    }

    @Override
    public Pool createPool(Vertx vertx, DatabaseSource source) {
        var connectOptions = MySQLConnectOptions.fromUri(SqlClientProvider.toConnectionUri(source.getJdbcDsn()));

        if (source.getUsername() != null) {
            connectOptions.setUser(source.getUsername());
        }
        if (source.getPassword() != null) {
            connectOptions.setPassword(source.getPassword().toString());
        }

        LOG.debug(
                "Creating MySQL pool for {}:{}/{}",
                connectOptions.getHost(),
                connectOptions.getPort(),
                connectOptions.getDatabase());

        return Pool.pool(vertx, connectOptions, new PoolOptions().setMaxSize(DEFAULT_POOL_SIZE));
    }

    @Override
    public SQLDialect dialect() {
        return SQLDialect.MYSQL;
    }

    @Override
    public Optional<IRI> mapNaturalDatatype(JDBCType jdbcType, String typeName) {
        var upperTypeName = typeName != null ? typeName.toUpperCase(Locale.ROOT) : "";
        var isUnsigned = upperTypeName.contains("UNSIGNED");

        return switch (jdbcType) {
            case TINYINT -> mapTinyInt(upperTypeName, isUnsigned);
            case SMALLINT -> Optional.of(isUnsigned ? XSD.UNSIGNED_SHORT : XSD.INTEGER);
            case INTEGER -> Optional.of(isUnsigned ? XSD.UNSIGNED_INT : XSD.INTEGER);
            case BIGINT -> Optional.of(isUnsigned ? XSD.UNSIGNED_LONG : XSD.INTEGER);
            case DECIMAL, NUMERIC -> Optional.of(XSD.DECIMAL);
            case FLOAT, REAL, DOUBLE -> Optional.of(XSD.DOUBLE);
            case BIT, BOOLEAN -> Optional.of(XSD.BOOLEAN);
            case DATE -> Optional.of(XSD.DATE);
            case TIME, TIME_WITH_TIMEZONE -> Optional.of(XSD.TIME);
            case TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> Optional.of(XSD.DATETIME);
            case BINARY, VARBINARY, LONGVARBINARY, BLOB -> Optional.of(XSD.HEXBINARY);
            default -> mapByTypeName(upperTypeName);
        };
    }

    private static Optional<IRI> mapTinyInt(String upperTypeName, boolean isUnsigned) {
        // MySQL TINYINT(1) and BOOL/BOOLEAN are boolean
        if (upperTypeName.contains("BOOL") || upperTypeName.contains("TINYINT(1)")) {
            return Optional.of(XSD.BOOLEAN);
        }
        return Optional.of(isUnsigned ? XSD.UNSIGNED_BYTE : XSD.BYTE);
    }

    /**
     * Fallback mapping for MySQL-specific types not covered by standard JDBC types.
     */
    private static Optional<IRI> mapByTypeName(String upperTypeName) {
        if (upperTypeName.contains("MEDIUMINT")) {
            return Optional.of(upperTypeName.contains("UNSIGNED") ? XSD.UNSIGNED_INT : XSD.INTEGER);
        }
        if (upperTypeName.equals("YEAR")) {
            return Optional.of(XSD.GYEAR);
        }
        if (upperTypeName.equals("DATETIME")) {
            return Optional.of(XSD.DATETIME);
        }
        if (upperTypeName.contains("BLOB") || upperTypeName.equals("VARBINARY")) {
            return Optional.of(XSD.HEXBINARY);
        }
        return Optional.empty();
    }
}
