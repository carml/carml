package io.carml.testcases.rml.ioregistry;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sql.MySqlResolver;
import io.carml.logicalsourceresolver.sql.PostgreSqlResolver;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.MySQLR2DBCDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.PostgreSQLR2DBCDatabaseContainer;

class TestRmlIoRegistryTestCases extends RmlTestCaseSuite {

    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

    private static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>("mysql:8").withUsername("root").withUrlParam("allowMultiQueries", "true");

    private static final PostgreSQLContainer<?> POSTGRESQL =
            new PostgreSQLContainer<>("postgres:latest").withUsername("root");

    @BeforeAll
    static void beforeAll() {
        MYSQL.start();
        POSTGRESQL.start();
    }

    @AfterAll
    static void afterAll() {
        MYSQL.stop();
        POSTGRESQL.stop();
    }

    @Override
    protected String getBasePath() {
        return "/rml/ioregistry/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // Expected error but mapping succeeds
                "RMLIOREGTC0002b",
                "RMLIOREGTC0002c",
                "RMLIOREGTC0003b",
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
                // Unknown column in DB query
                "RMLIOREGTC0004l",
                "RMLIOREGTC0005k",
                "RMLIOREGTC0005l",
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
                // SQL Server tests - not supported
                "RMLIOREGTC0006");
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
            prepareForDatabaseTest(
                    testCaseIdentifier,
                    mapperBuilder,
                    MYSQL,
                    mysql -> MySQLR2DBCDatabaseContainer.getOptions((MySQLContainer<?>) mysql));
        }

        if (mappingContent.contains(POSTGRESQL_DRIVER)) {
            mapperBuilder.excludeLogicalSourceResolver(MySqlResolver.NAME);
            prepareForDatabaseTest(
                    testCaseIdentifier,
                    mapperBuilder,
                    POSTGRESQL,
                    postgresql -> PostgreSQLR2DBCDatabaseContainer.getOptions((PostgreSQLContainer<?>) postgresql));
        }

        RdfRmlMapper mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        String.format("%s/%s", getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }
}
