package io.carml.logicalsourceresolver.sql;

import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.carml.logicalsourceresolver.LogicalSourceResolverException;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverSupplier;
import io.carml.logicalsourceresolver.MatchingLogicalSourceResolverSupplier;
import io.carml.logicalsourceresolver.sql.sourceresolver.JoiningDatabaseSource;
import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.vocab.Rdf;
import io.r2dbc.spi.Type;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
    if (sqlDataType instanceof MySqlType) {
      var mysqlType = (MySqlType) sqlDataType;
      switch (mysqlType) {
        case SMALLINT:
        case MEDIUMINT:
        case BIGINT:
        case INT:
          return XSD.INTEGER;
        case DECIMAL:
          return XSD.DECIMAL;
        case FLOAT:
        case DOUBLE:
          return XSD.DOUBLE;
        case DATE:
          return XSD.DATE;
        case TIME:
          return XSD.TIME;
        case YEAR:
          return XSD.GYEAR;
        case TIMESTAMP:
          return XSD.DATETIME;
        case TINYINT:
          if (((MySqlType) sqlDataType).getBinarySize() == 1) {
            return XSD.BOOLEAN;
          } else {
            return XSD.BYTE;
          }
        case SMALLINT_UNSIGNED:
          return XSD.UNSIGNED_SHORT;
        case MEDIUMINT_UNSIGNED:
        case INT_UNSIGNED:
          return XSD.UNSIGNED_INT;
        case BIGINT_UNSIGNED:
          return XSD.UNSIGNED_LONG;
        case VARBINARY:
        case TINYBLOB:
        case MEDIUMBLOB:
        case BLOB:
        case LONGBLOB:
          return XSD.HEXBINARY;
        case TINYINT_UNSIGNED:
          return XSD.UNSIGNED_BYTE;
        default:
          return XSD.STRING;
      }
    } else {
      throw new LogicalSourceResolverException(String.format("Encountered unsupported type: %s", sqlDataType));
    }
  }

  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Matcher implements MatchingLogicalSourceResolverSupplier {
    private static final Set<IRI> MATCHING_REF_FORMULATIONS = java.util.Set.of(Rdf.Ql.Rdb, Rdf.Rr.MySQL);

    private List<IRI> matchingReferenceFormulations;

    public static Matcher getInstance() {
      return getInstance(Set.of());
    }

    public static Matcher getInstance(Set<IRI> customMatchingReferenceFormulations) {
      return new Matcher(Stream.concat(customMatchingReferenceFormulations.stream(), MATCHING_REF_FORMULATIONS.stream())
          .distinct()
          .collect(Collectors.toUnmodifiableList()));
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
          .equals(Rdf.Rr.SQL2008) || logicalSource.getSqlVersion() != null
              && logicalSource.getSqlVersion()
                  .equals(Rdf.Rr.SQL2008);
    }

    private boolean hasMySqlSource(LogicalSource logicalSource) {
      if (logicalSource.getSource() instanceof DatabaseSource) {
        var dbSource = (DatabaseSource) logicalSource.getSource();
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
