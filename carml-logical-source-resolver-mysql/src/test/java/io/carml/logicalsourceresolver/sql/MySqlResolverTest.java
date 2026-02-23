package io.carml.logicalsourceresolver.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.model.ReferenceFormulation;
import io.carml.model.SqlReferenceFormulation;
import io.carml.vocab.Rdf.Rr;
import io.r2dbc.spi.Type;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MySqlResolverTest {

    @Nested
    class GetDatatypeIri {

        private final MySqlResolver resolver =
                (MySqlResolver) MySqlResolver.factory(true).apply(mock());

        static Stream<Arguments> mySqlTypeMappings() {
            return Stream.of(
                    Arguments.of(MySqlType.SMALLINT, XSD.INTEGER),
                    Arguments.of(MySqlType.MEDIUMINT, XSD.INTEGER),
                    Arguments.of(MySqlType.BIGINT, XSD.INTEGER),
                    Arguments.of(MySqlType.INT, XSD.INTEGER),
                    Arguments.of(MySqlType.DECIMAL, XSD.DECIMAL),
                    Arguments.of(MySqlType.FLOAT, XSD.DOUBLE),
                    Arguments.of(MySqlType.DOUBLE, XSD.DOUBLE),
                    Arguments.of(MySqlType.DATE, XSD.DATE),
                    Arguments.of(MySqlType.TIME, XSD.TIME),
                    Arguments.of(MySqlType.YEAR, XSD.GYEAR),
                    Arguments.of(MySqlType.TIMESTAMP, XSD.DATETIME),
                    Arguments.of(MySqlType.SMALLINT_UNSIGNED, XSD.UNSIGNED_SHORT),
                    Arguments.of(MySqlType.MEDIUMINT_UNSIGNED, XSD.UNSIGNED_INT),
                    Arguments.of(MySqlType.INT_UNSIGNED, XSD.UNSIGNED_INT),
                    Arguments.of(MySqlType.BIGINT_UNSIGNED, XSD.UNSIGNED_LONG),
                    Arguments.of(MySqlType.TINYINT_UNSIGNED, XSD.UNSIGNED_BYTE),
                    Arguments.of(MySqlType.VARBINARY, XSD.HEXBINARY),
                    Arguments.of(MySqlType.TINYBLOB, XSD.HEXBINARY),
                    Arguments.of(MySqlType.MEDIUMBLOB, XSD.HEXBINARY),
                    Arguments.of(MySqlType.BLOB, XSD.HEXBINARY),
                    Arguments.of(MySqlType.LONGBLOB, XSD.HEXBINARY),
                    Arguments.of(MySqlType.VARCHAR, XSD.STRING),
                    Arguments.of(MySqlType.TEXT, XSD.STRING),
                    Arguments.of(MySqlType.JSON, XSD.STRING));
        }

        @ParameterizedTest
        @MethodSource("mySqlTypeMappings")
        void givenMySqlType_thenReturnsExpectedXsdType(MySqlType mySqlType, IRI expectedXsd) {
            assertThat(resolver.getDatatypeIri(mySqlType), is(expectedXsd));
        }

        @Test
        void givenTinyintWithBinarySize1_thenReturnsBoolean() {
            // MySqlType.TINYINT has getBinarySize() == 1 by default
            assertThat(resolver.getDatatypeIri(MySqlType.TINYINT), is(XSD.BOOLEAN));
        }

        @Test
        void givenNonMySqlType_thenThrows() {
            Type unknownType = mock();
            assertThrows(LogicalSourceResolverException.class, () -> resolver.getDatatypeIri(unknownType));
        }
    }

    @Nested
    class MatcherTest {

        private final MySqlResolver.Matcher matcher = new MySqlResolver.Matcher();

        @Test
        void givenSqlReferenceFormulation_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            var refFormulation = mock(SqlReferenceFormulation.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var result = matcher.apply(logicalSource);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void givenMySqlSqlVersion_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getSqlVersion()).thenReturn(Rr.MySQL);

            var result = matcher.apply(logicalSource);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void givenMySqlJdbcDriver_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getJdbcDriver()).thenReturn("com.mysql.cj.jdbc.Driver");
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
            assertThat(matcher.getResolverName(), is("MySqlResolver"));
        }
    }
}
