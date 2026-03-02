package io.carml.logicalsourceresolver.sql;

import com.google.auto.service.AutoService;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverFactory;
import io.carml.logicalsourceresolver.sql.sourceresolver.JoiningDatabaseSource;
import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.model.Source;
import io.carml.model.SqlReferenceFormulation;
import io.carml.vocab.Rdf.Rr;
import io.r2dbc.mssql.message.type.SqlServerType;
import io.r2dbc.spi.Type;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.ToString;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.jooq.SQLDialect;

public class SqlServerResolver extends SqlResolver {

    public static final String NAME = "SqlServerResolver";

    private SqlServerResolver(Source source, boolean isStrict) {
        super(source, isStrict);
    }

    public static LogicalSourceResolverFactory<RowData> factory() {
        return factory(true);
    }

    public static LogicalSourceResolverFactory<RowData> factory(boolean isStrict) {
        return source -> new SqlServerResolver(source, isStrict);
    }

    @Override
    public String getQuery(LogicalSource logicalSource) {
        return SqlResolver.getQuery(SQLDialect.DEFAULT, logicalSource);
    }

    @Override
    public String getJointSqlQuery(JoiningDatabaseSource joiningDatabaseSourceSupplier) {
        return SqlResolver.getJointSqlQuery(SQLDialect.DEFAULT, joiningDatabaseSourceSupplier);
    }

    // Datatype mapping per https://www.w3.org/TR/r2rml/#dfn-natural-rdf-literal
    @Override
    public IRI getDatatypeIri(Type sqlDataType) {
        if (sqlDataType instanceof SqlServerType sqlServerType) {
            return switch (sqlServerType) {
                case TINYINT, SMALLINT, INTEGER, BIGINT -> XSD.INTEGER;
                case DECIMAL, NUMERIC, SMALLMONEY, MONEY -> XSD.DECIMAL;
                case FLOAT, REAL -> XSD.DOUBLE;
                case BIT -> XSD.BOOLEAN;
                case DATE -> XSD.DATE;
                case TIME -> XSD.TIME;
                case DATETIME, SMALLDATETIME, DATETIME2, DATETIMEOFFSET -> XSD.DATETIME;
                case BINARY, VARBINARY, VARBINARYMAX, IMAGE, TIMESTAMP -> XSD.HEXBINARY;
                default -> XSD.STRING;
            };
        }

        return XSD.STRING;
    }

    @ToString
    @AutoService(MatchingLogicalSourceResolverFactory.class)
    @SuppressWarnings("unused")
    public static class Matcher implements MatchingLogicalSourceResolverFactory {

        private final List<IRI> matchingReferenceFormulations = Stream.concat(
                        SqlReferenceFormulation.IRIS.stream(), Stream.of(Rr.MSSQLServer))
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

            if (hasSqlServerSource(logicalSource)) {
                scoreBuilder.strongMatch();
            }

            var matchScore = scoreBuilder.build();

            if (matchScore.getScore() == 0) {
                return Optional.empty();
            }

            return Optional.of(MatchedLogicalSourceResolverFactory.of(matchScore, SqlServerResolver.factory()));
        }

        private boolean matchesReferenceFormulation(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() instanceof SqlReferenceFormulation
                    || logicalSource.getSqlVersion() != null
                            && matchingReferenceFormulations.contains(logicalSource.getSqlVersion());
        }

        private boolean referenceFormulationMatchesByIri(LogicalSource logicalSource) {
            return logicalSource.getReferenceFormulation() != null
                            && logicalSource.getReferenceFormulation().getAsResource() instanceof IRI refIri
                            && matchingReferenceFormulations.contains(refIri)
                    || logicalSource.getSqlVersion() != null
                            && matchingReferenceFormulations.contains(logicalSource.getSqlVersion());
        }

        private boolean hasSqlServerSource(LogicalSource logicalSource) {
            if (logicalSource.getSource() instanceof DatabaseSource dbSource) {
                return dbSource.getJdbcDriver() != null
                        && dbSource.getJdbcDriver().contains("sqlserver");
            }

            return false;
        }

        @Override
        public String getResolverName() {
            return NAME;
        }
    }
}
