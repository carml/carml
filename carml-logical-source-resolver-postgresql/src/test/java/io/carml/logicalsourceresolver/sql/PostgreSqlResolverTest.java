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
import io.r2dbc.postgresql.codec.PostgresqlObjectId;
import io.r2dbc.spi.Type;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PostgreSqlResolverTest {

    @Nested
    class GetDatatypeIri {

        private final PostgreSqlResolver resolver =
                (PostgreSqlResolver) PostgreSqlResolver.factory(true).apply(mock());

        static Stream<Arguments> postgreSqlTypeMappings() {
            return Stream.of(
                    Arguments.of(PostgresqlObjectId.BOOL, XSD.BOOLEAN),
                    Arguments.of(PostgresqlObjectId.DATE, XSD.DATE),
                    Arguments.of(PostgresqlObjectId.FLOAT4, XSD.DOUBLE),
                    Arguments.of(PostgresqlObjectId.FLOAT8, XSD.DOUBLE),
                    Arguments.of(PostgresqlObjectId.INT2, XSD.INTEGER),
                    Arguments.of(PostgresqlObjectId.INT4, XSD.INTEGER),
                    Arguments.of(PostgresqlObjectId.INT8, XSD.INTEGER),
                    Arguments.of(PostgresqlObjectId.NUMERIC, XSD.DECIMAL),
                    Arguments.of(PostgresqlObjectId.TIME, XSD.TIME),
                    Arguments.of(PostgresqlObjectId.TIMESTAMP, XSD.DATETIME),
                    Arguments.of(PostgresqlObjectId.TIMESTAMPTZ, XSD.DATETIME),
                    Arguments.of(PostgresqlObjectId.BYTEA, XSD.HEXBINARY),
                    Arguments.of(PostgresqlObjectId.VARCHAR, XSD.STRING),
                    Arguments.of(PostgresqlObjectId.TEXT, XSD.STRING),
                    Arguments.of(PostgresqlObjectId.JSON, XSD.STRING),
                    Arguments.of(PostgresqlObjectId.JSONB, XSD.STRING));
        }

        @ParameterizedTest
        @MethodSource("postgreSqlTypeMappings")
        void givenPostgreSqlType_thenReturnsExpectedXsdType(Type postgreSqlType, IRI expectedXsd) {
            assertThat(resolver.getDatatypeIri(postgreSqlType), is(expectedXsd));
        }

        @Test
        void givenUnknownTypeName_thenReturnsString() {
            Type unknownType = mock();
            when(unknownType.getName()).thenReturn("unknown_type");
            assertThat(resolver.getDatatypeIri(unknownType), is(XSD.STRING));
        }
    }

    @Nested
    class MatcherTest {

        private final PostgreSqlResolver.Matcher matcher = new PostgreSqlResolver.Matcher();

        @Test
        void givenSqlReferenceFormulation_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            var refFormulation = mock(SqlReferenceFormulation.class);
            when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var result = matcher.apply(logicalSource);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void givenPostgreSqlSqlVersion_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            when(logicalSource.getSqlVersion()).thenReturn(Rr.PostgreSQL);

            var result = matcher.apply(logicalSource);

            assertThat(result.isPresent(), is(true));
        }

        @Test
        void givenPostgreSqlJdbcDriver_thenMatches() {
            var logicalSource = mock(LogicalSource.class);
            var dbSource = mock(DatabaseSource.class);
            when(dbSource.getJdbcDriver()).thenReturn("org.postgresql.Driver");
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
            assertThat(matcher.getResolverName(), is("PostgreSqlResolver"));
        }
    }
}
