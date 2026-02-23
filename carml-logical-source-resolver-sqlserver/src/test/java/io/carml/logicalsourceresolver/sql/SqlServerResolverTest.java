package io.carml.logicalsourceresolver.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.model.ReferenceFormulation;
import io.carml.model.SqlReferenceFormulation;
import io.carml.vocab.Rdf.Rr;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.spi.Type;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SqlServerResolverTest {

    @Nested
    class GetDatatypeIri {

        private final SqlServerResolver resolver =
                (SqlServerResolver) SqlServerResolver.factory(true).apply(mock());

        static Stream<Arguments> sqlServerTypeMappings() {
            return Stream.of(
                    Arguments.of(SqlServerType.TINYINT, XSD.INTEGER),
                    Arguments.of(SqlServerType.SMALLINT, XSD.INTEGER),
                    Arguments.of(SqlServerType.INTEGER, XSD.INTEGER),
                    Arguments.of(SqlServerType.BIGINT, XSD.INTEGER),
                    Arguments.of(SqlServerType.DECIMAL, XSD.DECIMAL),
                    Arguments.of(SqlServerType.NUMERIC, XSD.DECIMAL),
                    Arguments.of(SqlServerType.SMALLMONEY, XSD.DECIMAL),
                    Arguments.of(SqlServerType.MONEY, XSD.DECIMAL),
                    Arguments.of(SqlServerType.FLOAT, XSD.DOUBLE),
                    Arguments.of(SqlServerType.REAL, XSD.DOUBLE),
                    Arguments.of(SqlServerType.BIT, XSD.BOOLEAN),
                    Arguments.of(SqlServerType.DATE, XSD.DATE),
                    Arguments.of(SqlServerType.TIME, XSD.TIME),
                    Arguments.of(SqlServerType.DATETIME, XSD.DATETIME),
                    Arguments.of(SqlServerType.SMALLDATETIME, XSD.DATETIME),
                    Arguments.of(SqlServerType.DATETIME2, XSD.DATETIME),
                    Arguments.of(SqlServerType.DATETIMEOFFSET, XSD.DATETIME),
                    Arguments.of(SqlServerType.BINARY, XSD.HEXBINARY),
                    Arguments.of(SqlServerType.VARBINARY, XSD.HEXBINARY),
                    Arguments.of(SqlServerType.VARBINARYMAX, XSD.HEXBINARY),
                    Arguments.of(SqlServerType.IMAGE, XSD.HEXBINARY),
                    Arguments.of(SqlServerType.TIMESTAMP, XSD.HEXBINARY),
                    Arguments.of(SqlServerType.CHAR, XSD.STRING),
                    Arguments.of(SqlServerType.VARCHAR, XSD.STRING),
                    Arguments.of(SqlServerType.NCHAR, XSD.STRING),
                    Arguments.of(SqlServerType.NVARCHAR, XSD.STRING),
                    Arguments.of(SqlServerType.TEXT, XSD.STRING),
                    Arguments.of(SqlServerType.NTEXT, XSD.STRING),
                    Arguments.of(SqlServerType.GUID, XSD.STRING),
                    Arguments.of(SqlServerType.XML, XSD.STRING),
                    Arguments.of(SqlServerType.VARCHARMAX, XSD.STRING),
                    Arguments.of(SqlServerType.NVARCHARMAX, XSD.STRING),
                    Arguments.of(SqlServerType.SQL_VARIANT, XSD.STRING));
        }

        @ParameterizedTest
        @MethodSource("sqlServerTypeMappings")
        void givenSqlServerType_thenReturnsExpectedXsdType(SqlServerType sqlServerType, IRI expectedXsd) {
            assertThat(resolver.getDatatypeIri(sqlServerType), is(expectedXsd));
        }

        @Test
        void givenNonSqlServerType_thenReturnsString() {
            Type unknownType = mock();
            assertThat(resolver.getDatatypeIri(unknownType), is(XSD.STRING));
        }
    }

    @Nested
    class MatcherTest {

        private final SqlServerResolver.Matcher matcher = new SqlServerResolver.Matcher();

        @Test
        void givenSqlReferenceFormulation_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            var refFormulation = mock(SqlReferenceFormulation.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var result = matcher.apply(logicalSource);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void givenMsSqlServerSqlVersion_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getSqlVersion()).thenReturn(Rr.MSSQLServer);

            var result = matcher.apply(logicalSource);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void givenSqlServerJdbcDriver_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getJdbcDriver()).thenReturn("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            when(logicalSource.getSource()).thenReturn(dbSource);

            var result = matcher.apply(logicalSource);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void givenReferenceFormulationWithNonIriResource_thenReturnsEmpty() {
            var logicalSource = mock(LogicalSource.class);
            var refFormulation = mock(ReferenceFormulation.class);
            when(refFormulation.getAsResource()).thenReturn(mock(org.eclipse.rdf4j.model.BNode.class));
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var result = matcher.apply(logicalSource);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void givenNoMatchingProperties_thenReturnsEmpty() {
            var logicalSource = mock(LogicalSource.class);

            var result = matcher.apply(logicalSource);

            assertThat(result.isEmpty(), is(true));
        }

        @Test
        void getResolverName_returnsExpectedName() {
            assertThat(matcher.getResolverName(), is("SqlServerResolver"));
        }
    }
}
