package io.carml.logicalsourceresolver.sql;

import com.google.auto.service.AutoService;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
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

public class MySqlResolver extends SqlResolver {

    public static final String NAME = "MySqlResolver";

    private MySqlResolver(Source source, boolean isStrict) {
        super(source, isStrict);
    }

    public static LogicalSourceResolverFactory<RowData> factory() {
        return factory(true);
    }

    public static LogicalSourceResolverFactory<RowData> factory(boolean isStrict) {
        return source -> new MySqlResolver(source, isStrict);
    }

    @Override
    public String getQuery(LogicalSource logicalSource) {
        return SqlResolver.getQuery(SQLDialect.MYSQL, logicalSource);
    }

    @Override
    public String getJointSqlQuery(JoiningDatabaseSource joiningDatabaseSourceSupplier) {
        return SqlResolver.getJointSqlQuery(SQLDialect.MYSQL, joiningDatabaseSourceSupplier);
    }

    @Override
    public IRI getDatatypeIri(Type sqlDataType) {
        if (sqlDataType instanceof MySqlType mySqlType) {
            return switch (mySqlType) {
                case SMALLINT, MEDIUMINT, BIGINT, INT -> XSD.INTEGER;
                case DECIMAL -> XSD.DECIMAL;
                case FLOAT, DOUBLE -> XSD.DOUBLE;
                case DATE -> XSD.DATE;
                case TIME -> XSD.TIME;
                case YEAR -> XSD.GYEAR;
                case TIMESTAMP -> XSD.DATETIME;
                case TINYINT -> {
                    if (((MySqlType) sqlDataType).getBinarySize() == 1) {
                        yield XSD.BOOLEAN;
                    } else {
                        yield XSD.BYTE;
                    }
                }
                case SMALLINT_UNSIGNED -> XSD.UNSIGNED_SHORT;
                case MEDIUMINT_UNSIGNED, INT_UNSIGNED -> XSD.UNSIGNED_INT;
                case BIGINT_UNSIGNED -> XSD.UNSIGNED_LONG;
                case VARBINARY, TINYBLOB, MEDIUMBLOB, BLOB, LONGBLOB -> XSD.HEXBINARY;
                case TINYINT_UNSIGNED -> XSD.UNSIGNED_BYTE;
                default -> XSD.STRING;
            };
        } else {
            throw new LogicalSourceResolverException(String.format("Encountered unsupported type: %s", sqlDataType));
        }
    }

    @ToString
    @AutoService(MatchingLogicalSourceResolverFactory.class)
    public static class Matcher implements MatchingLogicalSourceResolverFactory {

        private final List<IRI> matchingReferenceFormulations = Stream.concat(
                        SqlReferenceFormulation.IRIS.stream(), Stream.of(Rr.MySQL))
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

            if (hasMySqlSource(logicalSource)) {
                scoreBuilder.strongMatch();
            }

            var matchScore = scoreBuilder.build();

            if (matchScore.getScore() == 0) {
                return Optional.empty();
            }

            return Optional.of(MatchedLogicalSourceResolverFactory.of(matchScore, MySqlResolver.factory()));
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

        private boolean hasMySqlSource(LogicalSource logicalSource) {
            if (logicalSource.getSource() instanceof DatabaseSource dbSource) {
                return dbSource.getJdbcDriver() != null
                        && dbSource.getJdbcDriver().contains("mysql");
            }

            return false;
        }

        @Override
        public String getResolverName() {
            return NAME;
        }
    }
}
