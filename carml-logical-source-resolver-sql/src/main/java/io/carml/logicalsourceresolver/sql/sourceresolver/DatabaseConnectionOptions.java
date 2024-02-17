package io.carml.logicalsourceresolver.sql.sourceresolver;

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(staticName = "of")
@Getter
public class DatabaseConnectionOptions {

  private static final String JDBC_SCHEME = "jdbc";

  private static final String R2DBC_SCHEME = "r2dbc";

  @NonNull
  private ConnectionFactoryOptions connectionFactoryOptions;

  public static Builder builder() {
    return new Builder();
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class Builder {
    private final ConnectionFactoryOptions.Builder optionsBuilder = ConnectionFactoryOptions.builder();

    private ConnectionFactoryOptions tmpDsnOptions;

    public Builder dsn(String dsn) {
      // TODO: this is a very naive implementation. Possibly need to to delegate to specific logical
      // source resolvers
      var processedUrl =
          dsn.startsWith(String.format("%s:", JDBC_SCHEME)) ? dsn.replaceFirst(JDBC_SCHEME, R2DBC_SCHEME) : dsn;

      var dsnOptions = ConnectionFactoryOptions.parse(processedUrl);

      tmpDsnOptions = dsnOptions;

      optionsBuilder.from(dsnOptions);

      return this;
    }

    public Builder driver(String driver) {
      checkAndWarn(ConnectionFactoryOptions.DRIVER);
      // TODO: Also delegate?
      optionsBuilder.option(ConnectionFactoryOptions.DRIVER,
          driver.equals("com.mysql.cj.jdbc.Driver") || driver.equals("com.mysql.jdbc.Driver") ? "mysql" : driver);

      return this;
    }

    public Builder host(String host) {
      checkAndWarn(ConnectionFactoryOptions.HOST);
      optionsBuilder.option(ConnectionFactoryOptions.HOST, host);

      return this;
    }

    public Builder database(String host) {
      checkAndWarn(ConnectionFactoryOptions.DATABASE);
      optionsBuilder.option(ConnectionFactoryOptions.DATABASE, host);

      return this;
    }

    public Builder username(String username) {
      checkAndWarn(ConnectionFactoryOptions.USER);
      optionsBuilder.option(ConnectionFactoryOptions.USER, username);

      return this;
    }

    public Builder password(CharSequence password) {
      checkAndWarn(ConnectionFactoryOptions.PASSWORD);
      optionsBuilder.option(ConnectionFactoryOptions.PASSWORD, password);

      return this;
    }

    public DatabaseConnectionOptions build() {
      return new DatabaseConnectionOptions(optionsBuilder.build());
    }

    private void checkAndWarn(Option<?> option) {
      if (tmpDsnOptions != null && tmpDsnOptions.hasOption(option)) {
        LOG.warn("Overriding option `{}` already derived from DSN.", option.name());
      }
    }
  }
}
