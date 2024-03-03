package io.carml.testcases.util;

import static io.carml.util.ModelSerializer.SIMPLE_WRITER_CONFIG;
import static java.util.function.Predicate.not;

import io.carml.testcases.model.Database;
import io.carml.testcases.model.Input;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.model.TripleStore;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;

public class ManifestGenerator {

  private static final Set<String> SQL_TYPE = Set.of("MySQL", "PostgreSQL");


  public static void main(String[] args) {
    try {
      generateManifest("rml/core/test-cases");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static void generateManifest(String resource) throws URISyntaxException {
    var path = Paths.get(ClassLoader.getSystemResource(resource).toURI());

    try (var testCasesDir = Files.walk(path, 1)) {
      var manifestModel = testCasesDir.filter(Files::isDirectory)
          .filter(not(path::equals))
          .flatMap(ManifestGenerator::processTestCase)
          .collect(ModelCollector.toTreeModel());

      var manifestPath = path.resolve("manifest.ttl");

      manifestModel.setNamespace(DCTERMS.NS);
      manifestModel.setNamespace("test", "http://www.w3.org/2006/03/test-description#");
      manifestModel.setNamespace("rmltest", "http://w3id.org/rml/test/");

      Rio.write(manifestModel, Files.newOutputStream(manifestPath), "http://w3id.org/rml/core/test-cases/id/",
          RDFFormat.TURTLE,
          SIMPLE_WRITER_CONFIG.apply(new WriterConfig()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stream<Statement> processTestCase(Path testcasePath) {
    var testCaseName = testcasePath.getFileName().toString();
    var testCaseBuilder = TestCase.builder()
        .id(String.format("http://w3id.org/rml/core/test-cases/id/%s", testCaseName))
        .identifier(testCaseName);

    var testCaseType = testCaseName.split("-")[1];
    var files = getFiles(testcasePath);
    var inputBuilder = Input.builder()
        .id(String.format("input-%s", testCaseName))
        .inputType(testCaseType);

    if (SQL_TYPE.contains(testCaseType)) {
      processSqlTestCase(inputBuilder, testCaseName, files);
    } else if (testCaseType.equals("SPARQL")) {
      processSparqlTestCase(inputBuilder, testCaseName, files);
    } else {
      processOtherTestCase(inputBuilder, files);
    }

    var mapping = files.stream()
        .map(path -> path.getFileName().toString())
        .filter(fileName -> fileName.equals("mapping.ttl"))
        .findFirst()
        .orElseThrow();

    var output = files.stream()
        .map(path -> path.getFileName().toString())
        .filter(fileName -> fileName.equals("output.nq"))
        .findFirst()
        .orElse(null);

    var testCase = testCaseBuilder
        .input(inputBuilder.build())
        .mappingDocument(mapping)
        .output(output)
        .hasExpectedOutput(output != null)
        .build();

    return testCase.asRdf().stream();
  }

  private static List<Path> getFiles(Path testcasePath) {
    try (var testCaseFiles = Files.walk(testcasePath)) {
      return testCaseFiles
          .filter(Files::isRegularFile)
          .toList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void processSqlTestCase(Input.InputBuilder<?, ?> inputBuilder, String testCaseName, List<Path> files) {
    var databaseBuilder = Database.builder()
        .id(String.format("database-%s", testCaseName));

    files.stream()
        .map(path -> path.getFileName().toString())
        .filter(fileName -> fileName.endsWith(".sql"))
        .forEach(databaseBuilder::sqlScriptFile);

    inputBuilder.database(databaseBuilder.build());
  }

  private static void processSparqlTestCase(Input.InputBuilder<?, ?> inputBuilder, String testCaseName,
      List<Path> files) {
    var tripleStoreBuilder = TripleStore.builder()
        .id(String.format("triplestore-%s", testCaseName));

    files.stream()
        .map(path -> path.getFileName().toString())
        .filter(fileName -> fileName.endsWith(".ttl"))
        .filter(not(fileName -> fileName.equals("mapping.ttl")))
        .forEach(tripleStoreBuilder::rdfFile);

    inputBuilder.tripleStore(tripleStoreBuilder.build());
  }

  private static void processOtherTestCase(Input.InputBuilder<?, ?> inputBuilder, List<Path> files) {
    files.stream()
        .map(path -> path.getFileName().toString())
        .filter(not(fileName -> fileName.endsWith(".sql")))
        .filter(not(fileName -> fileName.endsWith(".ttl")))
        .filter(not(fileName -> fileName.equals("output.nq")))
        .forEach(inputBuilder::inputFile);
  }
}
