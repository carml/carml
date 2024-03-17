package io.carml.logicalsourceresolver.sql.sourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

class DatabaseConnectionOptionsTest {

    @Test
    void givenInputOptions_whenBuild_thenGetExpectedOptions() {
        // Given
        var builder = DatabaseConnectionOptions.builder()
                .dsn("jdbc:mysql://localhost/example")
                .username("foo")
                .password("bar");

        // When
        var databaseConnectionOptions = builder.build();

        // Then
        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.DRIVER),
                is("mysql"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.HOST),
                is("localhost"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.DATABASE),
                is("example"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.USER),
                is("foo"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.PASSWORD),
                is("bar"));
    }

    @Test
    void givenOptionsThenDsn_whenBuild_thenGetExpectedOverriddenOptions() {
        // Given
        var builder = DatabaseConnectionOptions.builder()
                .driver("myDriver")
                .host("myHost")
                .database("myDatabase")
                .username("foo")
                .password("bar")
                .dsn("jdbc:mysql://localhost/example");

        // When
        var databaseConnectionOptions = builder.build();

        // Then
        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.DRIVER),
                is("mysql"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.HOST),
                is("localhost"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.DATABASE),
                is("example"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.USER),
                is("foo"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.PASSWORD),
                is("bar"));
    }

    @Test
    void givenDsnThenOptions_whenBuild_thenGetExpectedOverriddenOptions() {
        // Given
        var builder = DatabaseConnectionOptions.builder()
                .username("foo")
                .password("bar")
                .dsn("jdbc:mysql://localhost/example")
                .driver("myDriver")
                .host("myHost")
                .database("myDatabase");

        // When
        var databaseConnectionOptions = builder.build();

        // Then
        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.DRIVER),
                is("myDriver"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.HOST),
                is("myHost"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.DATABASE),
                is("myDatabase"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.USER),
                is("foo"));

        assertThat(
                databaseConnectionOptions.getConnectionFactoryOptions().getValue(ConnectionFactoryOptions.PASSWORD),
                is("bar"));
    }
}
