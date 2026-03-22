package io.carml.logicalview.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SqlClientProviderTest {

    @Nested
    class ToConnectionUri {

        @Test
        void toConnectionUri_mysqlJdbcUrl_stripsJdbcPrefix() {
            assertThat(SqlClientProvider.toConnectionUri("jdbc:mysql://host:3306/db"), is("mysql://host:3306/db"));
        }

        @Test
        void toConnectionUri_postgresqlJdbcUrl_stripsJdbcPrefix() {
            assertThat(
                    SqlClientProvider.toConnectionUri("jdbc:postgresql://host:5432/db"),
                    is("postgresql://host:5432/db"));
        }

        @Test
        void toConnectionUri_urlWithQueryParams_preservesParams() {
            assertThat(
                    SqlClientProvider.toConnectionUri("jdbc:mysql://host/db?useSSL=false"),
                    is("mysql://host/db?useSSL=false"));
        }

        @Test
        void toConnectionUri_nullUrl_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> SqlClientProvider.toConnectionUri(null));
        }

        @Test
        void toConnectionUri_nonJdbcUrl_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class, () -> SqlClientProvider.toConnectionUri("mysql://host/db"));
        }
    }
}
