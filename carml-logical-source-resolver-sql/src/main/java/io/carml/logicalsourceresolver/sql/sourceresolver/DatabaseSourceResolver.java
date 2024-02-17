package io.carml.logicalsourceresolver.sql.sourceresolver;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

import io.carml.logicalsourceresolver.sourceresolver.SourceResolver;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolverException;
import io.carml.model.DatabaseSource;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class DatabaseSourceResolver implements SourceResolver {

  private DatabaseConnectionOptions providedDatabaseConnectionOptions;

  public static DatabaseSourceResolver of() {
    return new DatabaseSourceResolver(null);
  }

  public static DatabaseSourceResolver of(DatabaseConnectionOptions databaseConnectionOptions) {
    return new DatabaseSourceResolver(databaseConnectionOptions);
  }

  @Override
  public boolean supportsSource(Object sourceObject) {
    return sourceObject instanceof DatabaseSource || sourceObject instanceof JoiningDatabaseSource;
  }

  @Override
  public Optional<Object> apply(Object source) {
    DatabaseSource resolvableSource;

    if (source instanceof JoiningDatabaseSource) {
      var joiningSourceSupplier = ((JoiningDatabaseSource) source);
      resolvableSource = (DatabaseSource) joiningSourceSupplier.getChildLogicalSource()
          .getSource();
    } else {
      resolvableSource = (DatabaseSource) source;
    }

    var connectionOpts = getDatabaseConnectionOptions(resolvableSource);
    var connectionFactoryOptions = connectionOpts.getConnectionFactoryOptions();

    checkConnectionOptions(connectionFactoryOptions);

    return Optional.of(connectionFactoryOptions);
  }

  private void checkConnectionOptions(ConnectionFactoryOptions opts) {
    if (!opts.hasOption(ConnectionFactoryOptions.HOST)) {
      throw new SourceResolverException(String.format("No connectable database options provided: %s", opts));
    }
  }

  private DatabaseConnectionOptions getDatabaseConnectionOptions(DatabaseSource databaseSource) {
    if (providedDatabaseConnectionOptions != null) {
      var providedOpts = providedDatabaseConnectionOptions.getConnectionFactoryOptions();
      var mergeBuilder = providedOpts.mutate();

      if (!providedOpts.hasOption(DRIVER)) {
        mergeBuilder.option(DRIVER, databaseSource.getJdbcDsn());
      }

      if (!providedOpts.hasOption(USER)) {
        mergeBuilder.option(USER, databaseSource.getUsername());
      }

      if (!providedOpts.hasOption(PASSWORD)) {
        mergeBuilder.option(PASSWORD, databaseSource.getPassword());
      }

      return DatabaseConnectionOptions.of(mergeBuilder.build());
    }

    var dbOptionsBuilder = DatabaseConnectionOptions.builder();

    if (databaseSource.getJdbcDsn() != null) {
      dbOptionsBuilder.dsn(databaseSource.getJdbcDsn());
    }

    if (databaseSource.getJdbcDriver() != null) {
      dbOptionsBuilder.driver(databaseSource.getJdbcDriver());
    }

    if (databaseSource.getUsername() != null) {
      dbOptionsBuilder.username(databaseSource.getUsername());
    }

    if (databaseSource.getPassword() != null) {
      dbOptionsBuilder.password(databaseSource.getPassword());
    }

    return dbOptionsBuilder.build();
  }
}
