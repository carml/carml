package io.carml.logicalview.sql.postgresql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.model.DatabaseSource;
import java.sql.JDBCType;
import java.util.Optional;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PostgreSqlClientProviderTest {

    private final PostgreSqlClientProvider provider = new PostgreSqlClientProvider();

    // --- supports() tests ---

    @Nested
    class Supports {

        @Test
        void supports_postgresqlDriver_returnsTrue() {
            var source = mock(DatabaseSource.class);
            when(source.getJdbcDriver()).thenReturn("org.postgresql.Driver");

            assertThat(provider.supports(source), is(true));
        }

        @Test
        void supports_mysqlDriver_returnsFalse() {
            var source = mock(DatabaseSource.class);
            when(source.getJdbcDriver()).thenReturn("com.mysql.cj.jdbc.Driver");

            assertThat(provider.supports(source), is(false));
        }

        @Test
        void supports_nullDriver_returnsFalse() {
            var source = mock(DatabaseSource.class);
            when(source.getJdbcDriver()).thenReturn(null);

            assertThat(provider.supports(source), is(false));
        }

        @Test
        void supports_unknownDriver_returnsFalse() {
            var source = mock(DatabaseSource.class);
            when(source.getJdbcDriver()).thenReturn("com.oracle.jdbc.OracleDriver");

            assertThat(provider.supports(source), is(false));
        }
    }

    // --- dialect() tests ---

    @Nested
    class Dialect {

        @Test
        void dialect_returnsPostgres() {
            assertThat(provider.dialect(), is(SQLDialect.POSTGRES));
        }
    }

    // --- mapNaturalDatatype() tests ---

    @Nested
    class MapNaturalDatatype {

        @Test
        void mapNaturalDatatype_smallint_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.SMALLINT, "int2"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_integer_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.INTEGER, "int4"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_bigint_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.BIGINT, "int8"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_decimal_returnsXsdDecimal() {
            assertThat(provider.mapNaturalDatatype(JDBCType.DECIMAL, "numeric"), is(Optional.of(XSD.DECIMAL)));
        }

        @Test
        void mapNaturalDatatype_float_returnsXsdDouble() {
            assertThat(provider.mapNaturalDatatype(JDBCType.FLOAT, "float4"), is(Optional.of(XSD.DOUBLE)));
        }

        @Test
        void mapNaturalDatatype_double_returnsXsdDouble() {
            assertThat(provider.mapNaturalDatatype(JDBCType.DOUBLE, "float8"), is(Optional.of(XSD.DOUBLE)));
        }

        @Test
        void mapNaturalDatatype_boolean_returnsXsdBoolean() {
            assertThat(provider.mapNaturalDatatype(JDBCType.BOOLEAN, "bool"), is(Optional.of(XSD.BOOLEAN)));
        }

        @Test
        void mapNaturalDatatype_date_returnsXsdDate() {
            assertThat(provider.mapNaturalDatatype(JDBCType.DATE, "date"), is(Optional.of(XSD.DATE)));
        }

        @Test
        void mapNaturalDatatype_time_returnsXsdTime() {
            assertThat(provider.mapNaturalDatatype(JDBCType.TIME, "time"), is(Optional.of(XSD.TIME)));
        }

        @Test
        void mapNaturalDatatype_timeWithTimezone_returnsXsdTime() {
            assertThat(provider.mapNaturalDatatype(JDBCType.TIME_WITH_TIMEZONE, "timetz"), is(Optional.of(XSD.TIME)));
        }

        @Test
        void mapNaturalDatatype_timestamp_returnsXsdDatetime() {
            assertThat(provider.mapNaturalDatatype(JDBCType.TIMESTAMP, "timestamp"), is(Optional.of(XSD.DATETIME)));
        }

        @Test
        void mapNaturalDatatype_timestampWithTimezone_returnsXsdDatetime() {
            assertThat(
                    provider.mapNaturalDatatype(JDBCType.TIMESTAMP_WITH_TIMEZONE, "timestamptz"),
                    is(Optional.of(XSD.DATETIME)));
        }

        @Test
        void mapNaturalDatatype_binary_returnsXsdHexBinary() {
            assertThat(provider.mapNaturalDatatype(JDBCType.BINARY, "bytea"), is(Optional.of(XSD.HEXBINARY)));
        }

        @Test
        void mapNaturalDatatype_serial_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.VARCHAR, "SERIAL"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_bigserial_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.VARCHAR, "BIGSERIAL"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_varchar_returnsEmpty() {
            assertThat(provider.mapNaturalDatatype(JDBCType.VARCHAR, "varchar").isEmpty(), is(true));
        }

        @Test
        void mapNaturalDatatype_uuid_returnsEmpty() {
            assertThat(provider.mapNaturalDatatype(JDBCType.VARCHAR, "uuid").isEmpty(), is(true));
        }
    }
}
