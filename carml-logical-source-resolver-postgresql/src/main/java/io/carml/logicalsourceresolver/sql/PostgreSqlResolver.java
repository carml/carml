package io.carml.logicalsourceresolver.sql;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BYTEA;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.DATE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NUMERIC;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIME;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMPTZ;

import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverSupplier;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverSupplier;
import io.carml.logicalsourceresolver.sql.sourceresolver.JoiningDatabaseSource;
import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.vocab.Rdf.Ql;
import io.carml.vocab.Rdf.Rml;
import io.carml.vocab.Rdf.Rr;
import io.r2dbc.spi.Type;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.jooq.SQLDialect;

public class PostgreSqlResolver extends SqlResolver {

    private PostgreSqlResolver(boolean strictness) {
        super(strictness);
    }

    public static PostgreSqlResolver getInstance() {
        return getInstance(true);
    }

    public static PostgreSqlResolver getInstance(boolean strictness) {
        return new PostgreSqlResolver(strictness);
    }

    @Override
    public String getQuery(LogicalSource logicalSource) {
        return SqlResolver.getQuery(SQLDialect.POSTGRES, logicalSource);
    }

    @Override
    public String getJointSqlQuery(JoiningDatabaseSource joiningDatabaseSourceSupplier) {
        return SqlResolver.getJointSqlQuery(SQLDialect.POSTGRES, joiningDatabaseSourceSupplier);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public IRI getDatatypeIri(Type sqlDataType) {
        var postgreSqlType = sqlDataType.getName();

        if (postgreSqlType.equals(BOOL.getName())) {
            return XSD.BOOLEAN;
        }
        if (postgreSqlType.equals(DATE.getName())) {
            return XSD.DATE;
        }
        if (postgreSqlType.equals(FLOAT4.getName()) || postgreSqlType.equals(FLOAT8.getName())) {
            return XSD.DOUBLE;
        }
        if (postgreSqlType.equals(INT2.getName())
                || postgreSqlType.equals(INT4.getName())
                || postgreSqlType.equals(INT8.getName())) {
            return XSD.INTEGER;
        }
        if (postgreSqlType.equals(NUMERIC.getName())) {
            return XSD.DECIMAL;
        }
        if (postgreSqlType.equals(TIME.getName())) {
            return XSD.TIME;
        }
        if (postgreSqlType.equals(TIMESTAMP.getName()) || postgreSqlType.equals(TIMESTAMPTZ.getName())) {
            return XSD.DATETIME;
        }
        if (postgreSqlType.equals(BYTEA.getName())) {
            return XSD.HEXBINARY;
        }

        return XSD.STRING;
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Matcher implements MatchingLogicalSourceResolverSupplier {

        private static final Set<IRI> MATCHING_REF_FORMULATIONS =
                Set.of(Rml.SQL2008Table, Rml.SQL2008Query, Ql.Rdb, Rr.PostgreSQL);

        private List<IRI> matchingReferenceFormulations;

        public static Matcher getInstance() {
            return getInstance(Set.of());
        }

        public static Matcher getInstance(Set<IRI> customMatchingReferenceFormulations) {
            return new Matcher(
                    Stream.concat(customMatchingReferenceFormulations.stream(), MATCHING_REF_FORMULATIONS.stream())
                            .distinct()
                            .toList());
        }

        @Override
        public Optional<MatchedLogicalSourceResolverSupplier> apply(LogicalSource logicalSource) {
            var scoreBuilder = MatchedLogicalSourceResolverSupplier.MatchScore.builder();

            if (matchesReferenceFormulation(logicalSource)) {
                scoreBuilder.strongMatch();
            }

            if (referenceFormulationMatchesSql2008(logicalSource)) {
                scoreBuilder.weakMatch();
            }

            if (hasMySqlSource(logicalSource)) {
                scoreBuilder.strongMatch();
            }

            var matchScore = scoreBuilder.build();

            if (matchScore.getScore() == 0) {
                return Optional.empty();
            }

            return Optional.of(MatchedLogicalSourceResolverSupplier.of(matchScore, PostgreSqlResolver::getInstance));
        }

        private boolean matchesReferenceFormulation(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() != null
                            && matchingReferenceFormulations.contains(logicalSource.getReferenceFormulation())
                    || logicalSource.getSqlVersion() != null
                            && matchingReferenceFormulations.contains(logicalSource.getSqlVersion());
        }

        private boolean referenceFormulationMatchesSql2008(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() != null
                            && logicalSource.getReferenceFormulation().equals(Rr.SQL2008)
                    || logicalSource.getSqlVersion() != null
                            && logicalSource.getSqlVersion().equals(Rr.SQL2008);
        }

        private boolean hasMySqlSource(LogicalSource logicalSource) {
            if (logicalSource.getSource() instanceof DatabaseSource dbSource) {
                return dbSource.getJdbcDriver() != null
                        && dbSource.getJdbcDriver().contains("postgresql");
            }

            return false;
        }

        @Override
        public String getResolverName() {
            return "PostgreSqlResolver";
        }
    }
}
