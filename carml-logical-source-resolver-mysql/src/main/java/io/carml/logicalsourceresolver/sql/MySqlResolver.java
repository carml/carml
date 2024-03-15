package io.carml.logicalsourceresolver.sql;

import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
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

public class MySqlResolver extends SqlResolver {

  private MySqlResolver(boolean strictness) {
    super(strictness);
  }

  public static MySqlResolver getInstance() {
    return getInstance(true);
  }

  public static MySqlResolver getInstance(boolean strictness) {
    return new MySqlResolver(strictness);
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

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Matcher implements MatchingLogicalSourceResolverSupplier {

    private static final Set<IRI> MATCHING_REF_FORMULATIONS =
        Set.of(Rml.SQL2008Table, Rml.SQL2008Query, Ql.Rdb, Rr.MySQL);

    private List<IRI> matchingReferenceFormulations;

    public static Matcher getInstance() {
      return getInstance(Set.of());
    }

    public static Matcher getInstance(Set<IRI> customMatchingReferenceFormulations) {
      return new Matcher(Stream.concat(customMatchingReferenceFormulations.stream(), MATCHING_REF_FORMULATIONS.stream())
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

      return Optional.of(MatchedLogicalSourceResolverSupplier.of(matchScore, MySqlResolver::getInstance));
    }

    private boolean matchesReferenceFormulation(LogicalSource logicalSource) {
      return logicalSource.getReferenceFormulation() != null
          && matchingReferenceFormulations.contains(logicalSource.getReferenceFormulation())
          || logicalSource.getSqlVersion() != null
              && matchingReferenceFormulations.contains(logicalSource.getSqlVersion());
    }

    private boolean referenceFormulationMatchesSql2008(LogicalSource logicalSource) {
      return logicalSource.getReferenceFormulation() != null && logicalSource.getReferenceFormulation()
          .equals(Rr.SQL2008) || logicalSource.getSqlVersion() != null
              && logicalSource.getSqlVersion()
                  .equals(Rr.SQL2008);
    }

    private boolean hasMySqlSource(LogicalSource logicalSource) {
      if (logicalSource.getSource() instanceof DatabaseSource dbSource) {
        return dbSource.getJdbcDriver() != null && dbSource.getJdbcDriver()
            .contains("mysql");
      }

      return false;
    }

    @Override
    public String getResolverName() {
      return "MySqlResolver";
    }
  }
}
