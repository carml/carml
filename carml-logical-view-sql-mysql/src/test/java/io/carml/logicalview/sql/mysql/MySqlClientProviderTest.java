package io.carml.logicalview.sql.mysql;

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

class MySqlClientProviderTest {

    private final MySqlClientProvider provider = new MySqlClientProvider();

    // --- supports() tests ---

    @Nested
    class Supports {

        @Test
        void supports_mysqlCjDriver_returnsTrue() {
            var source = mock(DatabaseSource.class);
            when(source.getJdbcDriver()).thenReturn("com.mysql.cj.jdbc.Driver");

            assertThat(provider.supports(source), is(true));
        }

        @Test
        void supports_legacyMysqlDriver_returnsTrue() {
            var source = mock(DatabaseSource.class);
            when(source.getJdbcDriver()).thenReturn("com.mysql.jdbc.Driver");

            assertThat(provider.supports(source), is(true));
        }

        @Test
        void supports_postgresDriver_returnsFalse() {
            var source = mock(DatabaseSource.class);
            when(source.getJdbcDriver()).thenReturn("org.postgresql.Driver");

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
        void dialect_returnsMysql() {
            assertThat(provider.dialect(), is(SQLDialect.MYSQL));
        }
    }

    // --- mapNaturalDatatype() tests ---

    @Nested
    class MapNaturalDatatype {

        @Test
        void mapNaturalDatatype_tinyint_returnsXsdByte() {
            assertThat(provider.mapNaturalDatatype(JDBCType.TINYINT, "TINYINT"), is(Optional.of(XSD.BYTE)));
        }

        @Test
        void mapNaturalDatatype_tinyint1_returnsXsdBoolean() {
            assertThat(provider.mapNaturalDatatype(JDBCType.TINYINT, "TINYINT(1)"), is(Optional.of(XSD.BOOLEAN)));
        }

        @Test
        void mapNaturalDatatype_tinyintUnsigned_returnsXsdUnsignedByte() {
            assertThat(
                    provider.mapNaturalDatatype(JDBCType.TINYINT, "TINYINT UNSIGNED"),
                    is(Optional.of(XSD.UNSIGNED_BYTE)));
        }

        @Test
        void mapNaturalDatatype_bit_returnsXsdBoolean() {
            assertThat(provider.mapNaturalDatatype(JDBCType.BIT, "BIT"), is(Optional.of(XSD.BOOLEAN)));
        }

        @Test
        void mapNaturalDatatype_boolean_returnsXsdBoolean() {
            assertThat(provider.mapNaturalDatatype(JDBCType.BOOLEAN, "BOOL"), is(Optional.of(XSD.BOOLEAN)));
        }

        @Test
        void mapNaturalDatatype_smallint_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.SMALLINT, "SMALLINT"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_smallintUnsigned_returnsXsdUnsignedShort() {
            assertThat(
                    provider.mapNaturalDatatype(JDBCType.SMALLINT, "SMALLINT UNSIGNED"),
                    is(Optional.of(XSD.UNSIGNED_SHORT)));
        }

        @Test
        void mapNaturalDatatype_int_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.INTEGER, "INT"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_intUnsigned_returnsXsdUnsignedInt() {
            assertThat(
                    provider.mapNaturalDatatype(JDBCType.INTEGER, "INT UNSIGNED"), is(Optional.of(XSD.UNSIGNED_INT)));
        }

        @Test
        void mapNaturalDatatype_mediumint_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.INTEGER, "MEDIUMINT"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_mediumintUnsigned_returnsXsdUnsignedInt() {
            assertThat(
                    provider.mapNaturalDatatype(JDBCType.INTEGER, "MEDIUMINT UNSIGNED"),
                    is(Optional.of(XSD.UNSIGNED_INT)));
        }

        @Test
        void mapNaturalDatatype_bigint_returnsXsdInteger() {
            assertThat(provider.mapNaturalDatatype(JDBCType.BIGINT, "BIGINT"), is(Optional.of(XSD.INTEGER)));
        }

        @Test
        void mapNaturalDatatype_bigintUnsigned_returnsXsdUnsignedLong() {
            assertThat(
                    provider.mapNaturalDatatype(JDBCType.BIGINT, "BIGINT UNSIGNED"),
                    is(Optional.of(XSD.UNSIGNED_LONG)));
        }

        @Test
        void mapNaturalDatatype_decimal_returnsXsdDecimal() {
            assertThat(provider.mapNaturalDatatype(JDBCType.DECIMAL, "DECIMAL"), is(Optional.of(XSD.DECIMAL)));
        }

        @Test
        void mapNaturalDatatype_float_returnsXsdDouble() {
            assertThat(provider.mapNaturalDatatype(JDBCType.FLOAT, "FLOAT"), is(Optional.of(XSD.DOUBLE)));
        }

        @Test
        void mapNaturalDatatype_double_returnsXsdDouble() {
            assertThat(provider.mapNaturalDatatype(JDBCType.DOUBLE, "DOUBLE"), is(Optional.of(XSD.DOUBLE)));
        }

        @Test
        void mapNaturalDatatype_date_returnsXsdDate() {
            assertThat(provider.mapNaturalDatatype(JDBCType.DATE, "DATE"), is(Optional.of(XSD.DATE)));
        }

        @Test
        void mapNaturalDatatype_time_returnsXsdTime() {
            assertThat(provider.mapNaturalDatatype(JDBCType.TIME, "TIME"), is(Optional.of(XSD.TIME)));
        }

        @Test
        void mapNaturalDatatype_timestamp_returnsXsdDatetime() {
            assertThat(provider.mapNaturalDatatype(JDBCType.TIMESTAMP, "TIMESTAMP"), is(Optional.of(XSD.DATETIME)));
        }

        @Test
        void mapNaturalDatatype_datetime_returnsXsdDatetime() {
            assertThat(provider.mapNaturalDatatype(JDBCType.TIMESTAMP, "DATETIME"), is(Optional.of(XSD.DATETIME)));
        }

        @Test
        void mapNaturalDatatype_year_returnsXsdGYear() {
            assertThat(provider.mapNaturalDatatype(JDBCType.VARCHAR, "YEAR"), is(Optional.of(XSD.GYEAR)));
        }

        @Test
        void mapNaturalDatatype_blob_returnsXsdHexBinary() {
            assertThat(provider.mapNaturalDatatype(JDBCType.BLOB, "BLOB"), is(Optional.of(XSD.HEXBINARY)));
        }

        @Test
        void mapNaturalDatatype_varbinary_returnsXsdHexBinary() {
            assertThat(provider.mapNaturalDatatype(JDBCType.VARBINARY, "VARBINARY"), is(Optional.of(XSD.HEXBINARY)));
        }

        @Test
        void mapNaturalDatatype_varchar_returnsEmpty() {
            assertThat(provider.mapNaturalDatatype(JDBCType.VARCHAR, "VARCHAR").isEmpty(), is(true));
        }
    }
}
