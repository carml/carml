package io.carml.testcases.rml.ioregistry;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sql.MySqlResolver;
import io.carml.logicalsourceresolver.sql.PostgreSqlResolver;
import io.carml.logicalsourceresolver.sql.SqlServerResolver;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseConnectionOptions;
import io.carml.model.TriplesMap;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.RmlTestCaseSuite;
import io.carml.util.RmlMappingLoader;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.MySQLR2DBCDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.PostgreSQLR2DBCDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;

class TestRmlIoRegistryTestCases extends RmlTestCaseSuite {

    // Strong reference prevents GC of the JUL logger (weakly referenced by default)
    @SuppressWarnings("unused")
    private static final java.util.logging.Logger MSSQL_JDBC_LOGGER =
            java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc");

    static {
        // Suppress noisy JDBC prelogin warnings during SQL Server container startup
        MSSQL_JDBC_LOGGER.setLevel(java.util.logging.Level.SEVERE);
    }

    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

    private static final String MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    @SuppressWarnings("resource")
    @Container
    private static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>("mysql:8").withUsername("root").withUrlParam("allowMultiQueries", "true");

    @SuppressWarnings("resource")
    @Container
    private static final PostgreSQLContainer<?> POSTGRESQL =
            new PostgreSQLContainer<>("postgres:latest").withUsername("root");

    @SuppressWarnings("resource")
    @Container
    private static final MSSQLServerContainer<?> MSSQL =
            new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2022-latest").acceptLicense();

    @BeforeAll
    static void beforeAll() {
        createMssqlTestDatabase();
    }

    private static void createMssqlTestDatabase() {
        try (Connection conn =
                DriverManager.getConnection(MSSQL.getJdbcUrl(), MSSQL.getUsername(), MSSQL.getPassword())) {
            conn.createStatement().execute("CREATE DATABASE TestDB");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create TestDB database", e);
        }
    }

    @Override
    protected String getBasePath() {
        return "/rml/ioregistry/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // Test case bug: spec says missing JSON/XML values produce NULL, not errors
                "RMLIOREGTC0002b",
                "RMLIOREGTC0003b",
                // Expected error for invalid PostgreSQL table
                "RMLIOREGTC0005d",
                // Output mismatch
                "RMLIOREGTC0003d",
                "RMLIOREGTC0004k",
                "RMLIOREGTC0004o",
                "RMLIOREGTC0004t",
                "RMLIOREGTC0004w",
                "RMLIOREGTC0004x",
                "RMLIOREGTC0005o",
                "RMLIOREGTC0005t",
                "RMLIOREGTC0005w",
                "RMLIOREGTC0005x",
                "RMLIOREGTC0006k",
                // Unknown column in DB query
                "RMLIOREGTC0004l",
                "RMLIOREGTC0005k",
                "RMLIOREGTC0005l",
                "RMLIOREGTC0006l",
                // Unsupported rdf type (wot:Thing)
                "RMLIOREGTC0008a",
                // Turtle parse error
                "RMLIOREGTC0009a",
                "RMLIOREGTC0010a",
                // Unsupported rdf type (sd:Service)
                "RMLIOREGTC0011a",
                // CSVW source not supported
                "RMLIOREGTC0012b",
                "RMLIOREGTC0012c",
                "RMLIOREGTC0012d",
                "RMLIOREGTC0012e",
                "RMLIOREGTC0012f",
                "RMLIOREGTC0012g",
                "RMLIOREGTC0012h",
                "RMLIOREGTC0012i",
                // Test case expected output incorrect: has plain "33" instead of "33"^^xsd:integer for JSON integers
                "RMLIOREGTC0007a",
                // SQL Server test case bug: invalid iterator "dsfjdlfjks;fkstudent" and output.nq references
                // unrelated Person/BirthDay data
                "RMLIOREGTC0006c",
                // SQL Server: expected error for invalid table not thrown (jOOQ DEFAULT dialect produces valid query)
                "RMLIOREGTC0006d",
                // SQL Server output mismatch: float scientific notation (3.0E1 vs 30.0)
                "RMLIOREGTC0006t",
                // SQL Server output mismatch: real/float scientific notation
                "RMLIOREGTC0006w",
                // SQL Server output mismatch: datetime trailing .0 (2009-10-10T12:12:22.0 vs 2009-10-10T12:12:22)
                "RMLIOREGTC0006x",
                // SQL Server output mismatch: binary hex encoding (double-encoded hex string)
                "RMLIOREGTC0006z");
    }

    private void prepareForDatabaseTest(
            String testCaseIdentifier,
            RdfRmlMapper.Builder mapperBuilder,
            JdbcDatabaseContainer<?> container,
            Function<JdbcDatabaseContainer<?>, ConnectionFactoryOptions> optionsGetter) {
        var sqlStream = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, "resource.sql");
        if (sqlStream != null) {
            try (Connection conn = DriverManager.getConnection(
                    container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
                var sql = new String(sqlStream.readAllBytes(), StandardCharsets.UTF_8);
                conn.createStatement().execute(sql);
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        mapperBuilder.databaseConnectionOptions(DatabaseConnectionOptions.of(optionsGetter.apply(container)));
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var mapperBuilder = RdfRmlMapper.builder().valueFactorySupplier(ValidatingValueFactory::new);

        byte[] mappingBytes;
        try {
            mappingBytes = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument())
                    .readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var mappingContent = new String(mappingBytes, StandardCharsets.UTF_8);
        Set<TriplesMap> mapping =
                RmlMappingLoader.build().load(RDFFormat.TURTLE, new ByteArrayInputStream(mappingBytes));

        if (mappingContent.contains(MYSQL_DRIVER)) {
            mapperBuilder.excludeLogicalSourceResolver(PostgreSqlResolver.NAME);
            mapperBuilder.excludeLogicalSourceResolver(SqlServerResolver.NAME);
            prepareForDatabaseTest(
                    testCaseIdentifier,
                    mapperBuilder,
                    MYSQL,
                    mysql -> MySQLR2DBCDatabaseContainer.getOptions((MySQLContainer<?>) mysql));
        }

        if (mappingContent.contains(POSTGRESQL_DRIVER)) {
            mapperBuilder.excludeLogicalSourceResolver(MySqlResolver.NAME);
            mapperBuilder.excludeLogicalSourceResolver(SqlServerResolver.NAME);
            prepareForDatabaseTest(
                    testCaseIdentifier,
                    mapperBuilder,
                    POSTGRESQL,
                    postgresql -> PostgreSQLR2DBCDatabaseContainer.getOptions((PostgreSQLContainer<?>) postgresql));
        }

        if (mappingContent.contains(MSSQL_DRIVER)) {
            mapperBuilder.excludeLogicalSourceResolver(MySqlResolver.NAME);
            mapperBuilder.excludeLogicalSourceResolver(PostgreSqlResolver.NAME);
            cleanMssqlTestDatabase();
            prepareForDatabaseTest(
                    testCaseIdentifier,
                    mapperBuilder,
                    MSSQL,
                    mssql -> getMssqlR2dbcOptions((MSSQLServerContainer<?>) mssql));
        }

        RdfRmlMapper mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    private static void cleanMssqlTestDatabase() {
        var jdbcUrl = MSSQL.getJdbcUrl() + ";databaseName=TestDB";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, MSSQL.getUsername(), MSSQL.getPassword())) {
            // Drop all foreign key constraints first to avoid dependency ordering issues
            conn.createStatement().execute("""
                            DECLARE @sql NVARCHAR(MAX) = '';
                            SELECT @sql += 'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
                                + ' DROP CONSTRAINT ' + QUOTENAME(f.name) + ';'
                            FROM sys.foreign_keys f
                            JOIN sys.tables t ON f.parent_object_id = t.object_id
                            JOIN sys.schemas s ON t.schema_id = s.schema_id;
                            EXEC sp_executesql @sql;
                            """);
            // Now drop all user tables
            conn.createStatement().execute("EXEC sp_msforeachtable 'DROP TABLE ?'");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clean TestDB database", e);
        }
    }

    private static ConnectionFactoryOptions getMssqlR2dbcOptions(MSSQLServerContainer<?> container) {
        return ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "mssql")
                .option(ConnectionFactoryOptions.HOST, container.getHost())
                .option(ConnectionFactoryOptions.PORT, container.getMappedPort(MSSQLServerContainer.MS_SQL_SERVER_PORT))
                .option(ConnectionFactoryOptions.USER, container.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, container.getPassword())
                .option(ConnectionFactoryOptions.DATABASE, "TestDB")
                .build();
    }
}
