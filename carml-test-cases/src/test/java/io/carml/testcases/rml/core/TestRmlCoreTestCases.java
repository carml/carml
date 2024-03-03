package io.carml.testcases.rml.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.CsvResolver;
import io.carml.logicalsourceresolver.JsonPathResolver;
import io.carml.logicalsourceresolver.XPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sql.MySqlResolver;
import io.carml.logicalsourceresolver.sql.PostgreSqlResolver;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseConnectionOptions;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.testcases.model.TestCase;
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
import java.util.stream.Stream;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.MySQLR2DBCDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.PostgreSQLR2DBCDatabaseContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

@Testcontainers
public class TestRmlCoreTestCases {

  private static final String BASE_PATH = "/rml/core/test-cases";

  private static final ValueFactory VF = SimpleValueFactory.getInstance();

  static final IRI TESTCASE = VF.createIRI("http://www.w3.org/2006/03/test-description#TestCase");

  static final List<String> SUPPORTED_SOURCE_TYPES = ImmutableList.of("CSV", "JSON", "XML", "MySQL", "PostgreSQL");

  private static final List<String> SKIP_TESTS = new ImmutableList.Builder<String>().build();

  public static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:latest").withUsername("root")
      .withUrlParam("allowMultiQueries", "true");

  public static PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:latest").withUsername("root")
      .withUrlParam("allowMultiQueries", "true");


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

  private RdfRmlMapper.Builder mapperBuilder;

  public static Stream<Arguments> populateTestCases() {
    var manifest = TestRmlCoreTestCases.class.getResourceAsStream(String.format("%s/manifest.ttl", BASE_PATH));
    return RdfObjectLoader.load(selectTestCases, TestCase.class, Models.parse(manifest, RDFFormat.TURTLE))
        .stream()
        .filter(TestRmlCoreTestCases::shouldBeTested)
        .sorted(Comparator.comparing(TestCase::getIdentifier))
        .map(testCase -> Arguments.of(testCase, testCase.getIdentifier()));
  }

  private static final Function<Model, Set<Resource>> selectTestCases =
      model -> model.filter(null, RDF.TYPE, TESTCASE)
          .subjects()
          .stream()
          .collect(Collectors.toUnmodifiableSet());

  private static boolean isSupported(TestCase testCase) {
    return SUPPORTED_SOURCE_TYPES.contains(testCase.getInput().getInputType());
  }

  private static boolean shouldBeTested(TestCase testCase) {
    return isSupported(testCase) && !SKIP_TESTS.contains(testCase.getIdentifier());
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("populateTestCases")
  void runTestCase(TestCase testCase, String testCaseIdentifier) {
    if (!testCase.hasExpectedOutput()) {
      // expect error
      assertThrows(RuntimeException.class, () -> executeMapping(testCase, testCaseIdentifier));
    } else {
      var result = executeMapping(testCase, testCaseIdentifier);

      InputStream expectedOutputStream = getTestCaseFileInputStream(BASE_PATH, testCaseIdentifier, testCase.getOutput());

      Model expected = Models.parse(expectedOutputStream, RDFFormat.NQUADS)
          .stream()
          .collect(ModelCollector.toTreeModel());

      assertThat(result, is(expected));
    }
  }

  private void prepareForDatabaseTest(TestCase testCase, String testCaseIdentifier, JdbcDatabaseContainer<?> container,
      Function<JdbcDatabaseContainer<?>, ConnectionFactoryOptions> optionsGetter) {
    testCase.getInput()
        .getDatabase()
        .getSqlScriptFiles()
        .stream()
        .map(sqlScript -> getTestCaseFileInputStream(BASE_PATH, testCaseIdentifier, sqlScript))
        .forEach(inputStream -> {
          try (Connection conn =
              DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword())) {
            var sql = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            conn.createStatement()
                .execute(sql);
          } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
          }
        });

    mapperBuilder.databaseConnectionOptions(DatabaseConnectionOptions.of(optionsGetter.apply(container)));
  }

  private Model executeMapping(TestCase testCase, String testCaseIdentifier) {
    mapperBuilder = RdfRmlMapper.builder()
        .valueFactorySupplier(ValidatingValueFactory::new)
        .logicalSourceResolverMatcher(CsvResolver.Matcher.getInstance())
        .logicalSourceResolverMatcher(JsonPathResolver.Matcher.getInstance())
        .logicalSourceResolverMatcher(XPathResolver.Matcher.getInstance());

    var mappingStream = getTestCaseFileInputStream(BASE_PATH, testCaseIdentifier, testCase.getMappingDocument());
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingStream);

    if (testCase.getInput().getInputType().equals("MySQL")) {
      mapperBuilder.logicalSourceResolverMatcher(MySqlResolver.Matcher.getInstance());
      prepareForDatabaseTest(testCase, testCaseIdentifier, mysql,
          mysql -> MySQLR2DBCDatabaseContainer.getOptions((MySQLContainer<?>) mysql));
    }

    if (testCase.getInput().getInputType().equals("PostgreSQL")) {
      mapperBuilder.logicalSourceResolverMatcher(PostgreSqlResolver.Matcher.getInstance());
      prepareForDatabaseTest(testCase, testCaseIdentifier, postgresql,
          postgresql -> PostgreSQLR2DBCDatabaseContainer.getOptions((PostgreSQLContainer<?>) postgresql));
    }

    RdfRmlMapper mapper = mapperBuilder.triplesMaps(mapping)
        .classPathResolver(
            ClassPathResolver.of(String.format("%s/%s", BASE_PATH, testCase.getIdentifier()),
                TestRmlCoreTestCases.class))
        .build();

    return mapper.map()
        .collect(ModelCollector.toTreeModel())
        .block();
  }

  static InputStream getTestCaseFileInputStream(String basePath, String testCaseIdentifier, String fileName) {
    return TestRmlCoreTestCases.class.getResourceAsStream(String.format("%s/%s/%s", basePath, testCaseIdentifier, fileName));
  }
}
