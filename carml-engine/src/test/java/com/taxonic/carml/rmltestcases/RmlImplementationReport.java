package com.taxonic.carml.rmltestcases;

import com.taxonic.carml.engine.rdf.RdfRmlMapper;
import com.taxonic.carml.logicalsourceresolver.CsvResolver;
import com.taxonic.carml.logicalsourceresolver.JsonPathResolver;
import com.taxonic.carml.logicalsourceresolver.XPathResolver;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.rdfmapper.util.RdfObjectLoader;
import com.taxonic.carml.rmltestcases.model.Output;
import com.taxonic.carml.rmltestcases.model.TestCase;
import com.taxonic.carml.util.Models;
import com.taxonic.carml.util.RdfCollectors;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.vocab.Rdf;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;

public class RmlImplementationReport {

  private static final String[] RESULT_HEADERS = {"testid", "result"};

  private static final String[] ERROR_HEADERS = {"id", "error"};

  public static void main(String[] args) {
    try (FileWriter results = new FileWriter("rml-implementation-report/results.csv");
        FileWriter errors = new FileWriter("rml-implementation-report/errors.csv")) {
      try (CSVPrinter resultPrinter = new CSVPrinter(results, CSVFormat.DEFAULT.withHeader(RESULT_HEADERS));
          CSVPrinter errorPrinter = new CSVPrinter(errors, CSVFormat.DEFAULT.withHeader(ERROR_HEADERS));) {
        populateTestCases().stream()
            .map(RmlImplementationReport::runTestCase)
            .forEach(result -> {
              try {
                resultPrinter.printRecord(result.getTestCase()
                    .getIdentifier(), result.getTestResult());
                String error = result.getErrorMessage();
                if (error != null) {
                  errorPrinter.printRecord(result.getTestCase()
                      .getIdentifier(), error);
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static Function<Model, Set<Resource>> selectTestCases =
      model -> Set.copyOf(model.filter(null, RDF.TYPE, TestRmlTestCases.EARL_TESTCASE)
          .subjects());


  public static Set<TestCase> populateTestCases() {
    InputStream metadata = RmlImplementationReport.class.getResourceAsStream("test-cases/metadata.nt");
    return RdfObjectLoader.load(selectTestCases, RmlTestCase.class, Models.parse(metadata, RDFFormat.NTRIPLES)) //
        .stream() //
        .collect(Collectors.toUnmodifiableSet());
  }

  private static boolean isSupported(String resource) {
    return TestRmlTestCases.SUPPORTED_SOURCE_TYPES.stream()//
        .anyMatch(resource::endsWith);
  }

  public static TestCaseResult runTestCase(TestCase testCase) {
    RdfRmlMapper.Builder mapperBuilder = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
        .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .classPathResolver(String.format("%s/%s", TestRmlTestCases.CLASS_LOCATION, testCase.getIdentifier()));

    Output expectedOutput = testCase.getOutput();
    boolean passed = false;

    TestCaseResult.Builder resultBuilder = TestCaseResult.builder()
        .testCase(testCase);

    if (!isSupported(testCase.getId())) {
      return resultBuilder.testResultInapplicable()
          .build();
    }

    if (expectedOutput.isError()) {
      try {
        executeMapping(testCase, mapperBuilder);
      } catch (Exception exception) {
        passed = true;
      }
    } else {
      try {
        Model result = executeMapping(testCase, mapperBuilder);
        InputStream expectedOutputStream = TestRmlTestCases.getDatasetInputStream(expectedOutput);

        Model expected = Models.parse(expectedOutputStream, RDFFormat.NQUADS)
            .stream() //
            .collect(RdfCollectors.toRdf4JTreeModel());

        passed = result.equals(expected);
      } catch (Exception exception) {
        passed = false;
        resultBuilder.errorMessage(exception.getLocalizedMessage());
      }
    }

    return resultBuilder.testResult(passed)
        .build();
  }

  private static Model executeMapping(TestCase testCase, RdfRmlMapper.Builder mapperBuilder) {
    InputStream mappingStream = TestRmlTestCases.getDatasetInputStream(testCase.getRules());
    Set<TriplesMap> mapping = RmlMappingLoader.build()
        .load(RDFFormat.TURTLE, mappingStream);

    RdfRmlMapper mapper = mapperBuilder.triplesMaps(mapping)
        .classPathResolver(String.format("%s/%s", TestRmlTestCases.CLASS_LOCATION, testCase.getIdentifier()))
        .build();

    return mapper.map()
        .collect(RdfCollectors.toRdf4JTreeModel())
        .block();
  }

  enum TestResult {
    inapplicable, failed, passed
  }

  static final class TestCaseResult {
    private final TestCase testCase;

    private final TestResult testResult;

    private final String errorMessage;

    private TestCaseResult(TestCase testCase, TestResult testResult, String errorMessage) {
      this.testCase = testCase;
      this.testResult = testResult;
      this.errorMessage = errorMessage;
    }

    static Builder builder() {
      return new Builder();
    }

    static class Builder {
      private TestCase testCase;

      private TestResult testResult;

      private String errorMessage;

      Builder testCase(TestCase testCase) {
        this.testCase = testCase;
        return this;
      }

      Builder testResultInapplicable() {
        this.testResult = TestResult.inapplicable;
        return this;
      }

      Builder testResult(boolean passed) {
        this.testResult = passed ? TestResult.passed : TestResult.failed;
        return this;
      }

      Builder errorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
      }

      TestCaseResult build() {
        return new TestCaseResult(testCase, testResult, errorMessage);
      }
    }

    public TestCase getTestCase() {
      return testCase;
    }

    public TestResult getTestResult() {
      return testResult;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }
}
