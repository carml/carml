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

import com.google.auto.service.AutoService;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.sql.sourceresolver.JoiningDatabaseSource;
import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.SqlReferenceFormulation;
import io.carml.vocab.Rdf.Rr;
import io.r2dbc.spi.Type;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.ToString;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.jooq.SQLDialect;

public class PostgreSqlResolver extends SqlResolver {

    public static final String NAME = "PostgreSqlResolver";

    private PostgreSqlResolver(Source source, boolean strictness) {
        super(source, strictness);
    }

    public static LogicalSourceResolverFactory<RowData> factory() {
        return factory(true);
    }

    public static LogicalSourceResolverFactory<RowData> factory(boolean isStrict) {
        return source -> new PostgreSqlResolver(source, isStrict);
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

    @ToString
    @AutoService(MatchingLogicalSourceResolverFactory.class)
    public static class Matcher implements MatchingLogicalSourceResolverFactory {

        private final List<IRI> matchingReferenceFormulations = Stream.concat(
                        SqlReferenceFormulation.IRIS.stream(), Stream.of(Rr.PostgreSQL))
                .toList();

        @Override
        public Optional<MatchedLogicalSourceResolverFactory> apply(LogicalSource logicalSource) {
            var scoreBuilder = MatchedLogicalSourceResolverFactory.MatchScore.builder();

            if (matchesReferenceFormulation(logicalSource)) {
                scoreBuilder.strongMatch();
            }

            if (referenceFormulationMatchesByIri(logicalSource)) {
                scoreBuilder.weakMatch();
            }

            if (hasPostgreSqlSource(logicalSource)) {
                scoreBuilder.strongMatch();
            }

            var matchScore = scoreBuilder.build();

            if (matchScore.getScore() == 0) {
                return Optional.empty();
            }

            return Optional.of(MatchedLogicalSourceResolverFactory.of(matchScore, PostgreSqlResolver.factory()));
        }

        private boolean matchesReferenceFormulation(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() instanceof SqlReferenceFormulation
                    || logicalSource.getSqlVersion() != null
                            && matchingReferenceFormulations.contains(logicalSource.getSqlVersion());
        }

        private boolean referenceFormulationMatchesByIri(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() != null
                            && matchingReferenceFormulations.contains(
                                    logicalSource.getReferenceFormulation().getAsResource())
                    || logicalSource.getSqlVersion() != null
                            && matchingReferenceFormulations.contains(logicalSource.getSqlVersion());
        }

        private boolean hasPostgreSqlSource(LogicalSource logicalSource) {
            if (logicalSource.getSource() instanceof DatabaseSource dbSource) {
                return dbSource.getJdbcDriver() != null
                        && dbSource.getJdbcDriver().contains("postgresql");
            }

            return false;
        }

        @Override
        public String getResolverName() {
            return NAME;
        }
    }
}
