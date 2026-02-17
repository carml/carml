package io.carml.rmltestcases;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sql.MySqlResolver;
import io.carml.logicalsourceresolver.sql.PostgreSqlResolver;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseConnectionOptions;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.rmltestcases.model.Dataset;
import io.carml.rmltestcases.model.Input;
import io.carml.rmltestcases.model.Output;
import io.carml.rmltestcases.model.TestCase;
import io.carml.util.Models;
import io.carml.util.RmlMappingLoader;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.MySQLR2DBCDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.PostgreSQLR2DBCDatabaseContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

@Testcontainers
public class TestRmlTestCases {

    public static MySQLContainer<?> mysql =
            new MySQLContainer<>("mysql:8").withUsername("root").withUrlParam("allowMultiQueries", "true");

    public static PostgreSQLContainer<?> postgresql =
            new PostgreSQLContainer<>("postgres:latest").withUsername("root").withUrlParam("allowMultiQueries", "true");

    @BeforeAll
    public static void beforeAll() {
        mysql.start();
        postgresql.start();
    }

    @AfterAll
    public static void afterAll() {
        mysql.stop();
        postgresql.stop();
    }

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    static final IRI EARL_TESTCASE = VF.createIRI("http://www.w3.org/ns/earl#TestCase");

    static final List<String> SUPPORTED_SOURCE_TYPES = ImmutableList.of("CSV", "JSON", "XML", "MySQL", "PostgreSQL");

    // Under discussion in https://github.com/RMLio/rml-test-cases/issues
    private static final List<String> SKIP_TESTS = new ImmutableList.Builder<String>() //
            // https://github.com/kg-construct/rml-test-cases/issues/12
            .add("RMLTC0002c-JSON")
            .add("RMLTC0002c-XML")
            // TODO
            .add("RMLTC0002f-MySQL")
            .add("RMLTC0002f-PostgreSQL")
            // https://github.com/kg-construct/rml-test-cases/issues/39
            .add("RMLTC0002i-MySQL")
            .add("RMLTC0002i-PostgreSQL")
            .add("RMLTC0002j-MySQL")
            .add("RMLTC0002j-PostgreSQL")
            // https://github.com/kg-construct/rml-test-cases/issues/13
            .add("RMLTC0007h-CSV")
            .add("RMLTC0007h-JSON")
            .add("RMLTC0007h-XML")
            .add("RMLTC0007h-MySQL")
            .add("RMLTC0007h-PostgreSQL")
            // https://github.com/kg-construct/rml-test-cases/issues/14
            .add("RMLTC0010a-JSON")
            .add("RMLTC0010b-JSON")
            .add("RMLTC0010c-JSON")
            // https://github.com/kg-construct/rml-test-cases/issues/15
            .add("RMLTC0015b-CSV")
            .add("RMLTC0015b-JSON")
            .add("RMLTC0015b-XML")
            // https://github.com/kg-construct/rml-test-cases/issues/16
            .add("RMLTC0019b-CSV")
            .add("RMLTC0019b-JSON")
            .add("RMLTC0019b-XML")
            .add("RMLTC0019b-MySQL")
            .add("RMLTC0019b-PostgreSQL")
            // https://github.com/kg-construct/rml-test-cases/issues/17
            .add("RMLTC0020b-CSV")
            .add("RMLTC0020b-JSON")
            .add("RMLTC0020b-XML")
            .add("RMLTC0020b-MySQL")
            .add("RMLTC0020b-PostgreSQL")
            // joining on different datatypes, not supported in PostgreSql
            .add("RMLTC0009a-PostgreSQL")
            .add("RMLTC0009b-PostgreSQL")
            // CARML supports multiple subjectMaps
            .add("RMLTC0012d-CSV")
            .add("RMLTC0012d-JSON")
            .add("RMLTC0012d-XML")
            .add("RMLTC0012d-MySQL")
            .add("RMLTC0012d-PostgreSQL")
            // blank node id issue double
            .add("RMLTC0012e-MySQL") // value generated is correct according to natural RDF lexical form
            .add("RMLTC0012e-PostgreSQL") // value generated is correct according to natural RDF lexical form
            // mapping uses rr:reference instead of rml:reference
            .add("RMLTC0013a-MySQL")
            .add("RMLTC0013a-PostgreSQL")
            // https://github.com/kg-construct/rml-test-cases/issues/42
            .add("RMLTC0015a-MySQL")
            .add("RMLTC0015a-PostgreSQL")
            // XML canonicalization of date adds time zone, which is not expected in test, but not wrong.
            .add("RMLTC0016c-MySQL")
            .add("RMLTC0016c-PostgreSQL")
            // postgres insert script incorrectly inserts hex values. Should be \x.. instead of \\x...
            .add("RMLTC0016e-PostgreSQL")
            // should drop table test.Student_Sport
            // mysql response doesn't pad string values to defined char size
            .add("RMLTC0018a-MySQL")
            // should drop table test.Student_Sport
            .add("RMLTC0020a-MySQL")
            .build();

    private RdfRmlMapper.Builder mapperBuilder;

    public static List<TestCase> populateTestCases() {
        InputStream metadata = TestRmlTestCases.class.getResourceAsStream("test-cases/metadata.nt");
        return RdfObjectLoader.load(selectTestCases, RmlTestCaze.class, Models.parse(metadata, RDFFormat.NTRIPLES))
                .stream()
                .filter(TestRmlTestCases::shouldBeTested)
                .sorted(Comparator.comparing(RmlTestCaze::getIdentifier))
                .collect(Collectors.toUnmodifiableList());
    }

    private static final Function<Model, Set<Resource>> selectTestCases =
            model -> model.filter(null, RDF.TYPE, EARL_TESTCASE).subjects().stream()
                    .filter(TestRmlTestCases::isSupported)
                    .collect(Collectors.toUnmodifiableSet());

    private static boolean isSupported(Resource resource) {
        return SUPPORTED_SOURCE_TYPES.stream() //
                .anyMatch(s -> resource.stringValue().endsWith(s));
    }

    private static boolean shouldBeTested(TestCase testCase) {
        return !SKIP_TESTS.contains(testCase.getIdentifier());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("populateTestCases")
    void runTestCase(TestCase testCase) {
        Output expectedOutput = testCase.getOutput();
        if (expectedOutput.isError()) {
            assertThrows(RuntimeException.class, () -> executeMapping(testCase));
        } else {
            Model result = executeMapping(testCase);

            InputStream expectedOutputStream = getDatasetInputStream(expectedOutput);

            Model expected = Models.parse(expectedOutputStream, RDFFormat.NQUADS).stream()
                    .collect(ModelCollector.toTreeModel());

            assertThat(result, is(expected));
        }
    }

    private void prepareForDatabaseTest(
            TestCase testCase,
            JdbcDatabaseContainer<?> container,
            Function<JdbcDatabaseContainer<?>, ConnectionFactoryOptions> optionsGetter) {
        testCase.getInput().stream().map(TestRmlTestCases::getInputInputStream).forEach(inputStream -> {
            try (Connection conn = DriverManager.getConnection(
                    container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
                var sql = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                conn.createStatement().execute(sql);
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        });

        mapperBuilder.databaseConnectionOptions(DatabaseConnectionOptions.of(optionsGetter.apply(container)));
    }

    private Model executeMapping(TestCase testCase) {
        mapperBuilder = RdfRmlMapper.builder().valueFactorySupplier(ValidatingValueFactory::new);

        InputStream mappingStream = getDatasetInputStream(testCase.getRules());
        Set<TriplesMap> mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        if (testCase.getId().endsWith("MySQL")) {
            mapperBuilder.excludeLogicalSourceResolver(PostgreSqlResolver.NAME);
            prepareForDatabaseTest(
                    testCase, mysql, mysql -> MySQLR2DBCDatabaseContainer.getOptions((MySQLContainer<?>) mysql));
        }

        if (testCase.getId().endsWith("PostgreSQL")) {
            mapperBuilder.excludeLogicalSourceResolver(MySqlResolver.NAME);
            prepareForDatabaseTest(
                    testCase,
                    postgresql,
                    postgresql -> PostgreSQLR2DBCDatabaseContainer.getOptions((PostgreSQLContainer<?>) postgresql));
        }

        RdfRmlMapper mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        String.format("test-cases/%s", testCase.getIdentifier()), TestRmlTestCases.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    static InputStream getDatasetInputStream(Dataset dataset) {
        String relativeLocation = dataset.getDistribution().getRelativeFileLocation();
        return TestRmlTestCases.class.getResourceAsStream(relativeLocation);
    }

    static InputStream getInputInputStream(Input input) {
        String relativeLocation = input.getDistribution().getRelativeFileLocation();
        return TestRmlTestCases.class.getResourceAsStream(relativeLocation);
    }
}
