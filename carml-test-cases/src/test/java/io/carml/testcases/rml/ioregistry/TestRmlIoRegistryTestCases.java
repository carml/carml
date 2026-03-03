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
                // Missing JSON/XML references produce NULL instead of error (hasError=true but CARML doesn't throw)
                "RMLIOREGTC0002b", // JSON: $.THIS_VALUE_DOES_NOT_EXIST
                "RMLIOREGTC0003b", // XML: NON_EXISTING element

                // Test case bug: namespace URL mismatch ("http://example.org" vs XML's "http://example.org/")
                // and unprefixed names in iterator/references for elements in the default namespace.
                // CARML's namespace pipeline works correctly (verified with corrected test case data).
                "RMLIOREGTC0003d",

                // Test case bug: hasError=true but mapping iterator "Person" matches the table created by
                // resource.sql — unlike 0004d (MySQL) which correctly uses non-existent "sdfdfsstudent"
                "RMLIOREGTC0005d", // PostgreSQL
                "RMLIOREGTC0006d", // SQL Server

                // Test case expected output has plain string "10" for SQL integer column
                // engine correctly produces "10"^^xsd:integer
                "RMLIOREGTC0004k", // MySQL
                "RMLIOREGTC0005k", // PostgreSQL
                "RMLIOREGTC0006k", // SQL Server

                // Test case expected output has non-canonical xsd:double ("30.0" vs canonical "3.0E1")
                // engine uses XMLDatatypeUtil.normalize which produces canonical scientific notation
                "RMLIOREGTC0004o", // MySQL: FLOAT amount column
                "RMLIOREGTC0004t", // MySQL: FLOAT amount column (two-table join)
                "RMLIOREGTC0005o", // PostgreSQL: FLOAT amount column
                "RMLIOREGTC0005t", // PostgreSQL: FLOAT amount column (two-table join)
                "RMLIOREGTC0006t", // SQL Server: FLOAT amount column (two-table join)

                // Test case expected output has non-canonical xsd:double for REAL/FLOAT Patient columns
                // (e.g. "80.25" vs canonical "8.025E1", "1.7" vs "1.7E0")
                "RMLIOREGTC0004w", // MySQL
                "RMLIOREGTC0005w", // PostgreSQL
                "RMLIOREGTC0006w", // SQL Server

                // SQL query references non-existent column "NoColumnName" but hasError=false (test case bug)
                "RMLIOREGTC0004l", // MySQL
                "RMLIOREGTC0005l", // PostgreSQL
                "RMLIOREGTC0006l", // SQL Server

                // Test case expected output has plain "33" for JSON integer; engine produces "33"^^xsd:integer
                "RMLIOREGTC0007a",

                // Unsupported source type: WoT (td:Thing) source descriptions
                "RMLIOREGTC0008a", // HTTP JSON API
                "RMLIOREGTC0009a", // Kafka stream
                "RMLIOREGTC0010a", // MQTT stream

                // Unsupported source type: SPARQL endpoint (sd:Service)
                "RMLIOREGTC0011a",

                // CSVW quoteChar test case bug: mapping references uppercase {ID}/{Name} but CSV headers are
                // lowercase id/name/age
                "RMLIOREGTC0012i",

                // SQL Server test case bug: invalid iterator "dsfjdlfjks;fkstudent" and output.nq references
                // unrelated Person/BirthDay data
                "RMLIOREGTC0006c",

                // Test case bug: resource.sql uses CAST('89504E47...' AS VARBINARY) which treats the hex
                // string as character data, storing ASCII bytes (0x38 0x39 0x35 ...) instead of the intended
                // raw binary (0x89 0x50 0x4E ...). This causes double-encoding when printHexBinary() hex-encodes
                // the ASCII bytes. The correct SQL Server syntax is a bare binary literal: 0x89504E470D0A...
                // (MySQL uses X'...' and PostgreSQL uses '\x...' which both work correctly)
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
