package com.taxonic.carml.rmltestcases;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.taxonic.carml.engine.rdf.RdfRmlMapper;
import com.taxonic.carml.engine.sourceresolver.ClassPathResolver;
import com.taxonic.carml.logicalsourceresolver.CsvResolver;
import com.taxonic.carml.logicalsourceresolver.JsonPathResolver;
import com.taxonic.carml.logicalsourceresolver.XPathResolver;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdfmapper.util.RdfObjectLoader;
import com.taxonic.carml.rmltestcases.model.Dataset;
import com.taxonic.carml.rmltestcases.model.Output;
import com.taxonic.carml.rmltestcases.model.TestCase;
import com.taxonic.carml.util.Models;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf;
import java.io.InputStream;
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
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestRmlTestCases {

  private static final ValueFactory VF = SimpleValueFactory.getInstance();

  static final IRI EARL_TESTCASE = VF.createIRI("http://www.w3.org/ns/earl#TestCase");

  static final List<String> SUPPORTED_SOURCE_TYPES = ImmutableList.of("CSV", "JSON", "XML");

  // Under discussion in https://github.com/RMLio/rml-test-cases/issues
  private static final List<String> SKIP_TESTS = new ImmutableList.Builder<String>().add("RMLTC0002c-JSON")
      .add("RMLTC0002c-XML")
      .add("RMLTC0007h-CSV")
      .add("RMLTC0007h-JSON")
      .add("RMLTC0007h-XML")
      .add("RMLTC0010a-JSON")
      .add("RMLTC0010b-JSON")
      .add("RMLTC0010c-JSON")
      .add("RMLTC0015b-CSV")
      .add("RMLTC0015b-JSON")
      .add("RMLTC0015b-XML")
      .add("RMLTC0019b-CSV")
      .add("RMLTC0019b-JSON")
      .add("RMLTC0019b-XML")
      .add("RMLTC0020b-CSV")
      .add("RMLTC0020b-JSON")
      .add("RMLTC0020b-XML")
      // multiple subjectMaps
      .add("RMLTC0012d-CSV")
      .add("RMLTC0012d-JSON")
      .add("RMLTC0012d-XML")
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
      model -> model.filter(null, RDF.TYPE, EARL_TESTCASE)
          .subjects()
          .stream()
          .filter(TestRmlTestCases::isSupported)
          .collect(Collectors.toUnmodifiableSet());

  private static boolean isSupported(Resource resource) {
    return SUPPORTED_SOURCE_TYPES.stream()//
        .anyMatch(s -> resource.stringValue()
            .endsWith(s));
  }

  private static boolean shouldBeTested(TestCase testCase) {
    return !SKIP_TESTS.contains(testCase.getIdentifier());
  }

  @BeforeEach
  public void prepare() {
    mapperBuilder = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
        .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance);
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

      Model expected = Models.parse(expectedOutputStream, RDFFormat.NQUADS)
          .stream()
          .collect(ModelCollector.toTreeModel());

      assertThat(result, is(expected));
    }
  }

  private Model executeMapping(TestCase testCase) {
    InputStream mappingStream = getDatasetInputStream(testCase.getRules());
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingStream);

    RdfRmlMapper mapper = mapperBuilder.triplesMaps(mapping)
        .classPathResolver(
            ClassPathResolver.of(String.format("test-cases/%s", testCase.getIdentifier()), TestRmlTestCases.class))
        .build();

    return mapper.map()
        .collect(ModelCollector.toTreeModel())
        .block();
  }

  static InputStream getDatasetInputStream(Dataset dataset) {
    String relativeLocation = dataset.getDistribution()
        .getRelativeFileLocation();
    return TestRmlTestCases.class.getResourceAsStream(relativeLocation);
  }
}
