package com.taxonic.carml.engine.iotests;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.Sets;
import com.taxonic.carml.engine.rdf.RdfRmlMapper;
import com.taxonic.carml.logicalsourceresolver.CsvResolver;
import com.taxonic.carml.logicalsourceresolver.JsonPathResolver;
import com.taxonic.carml.logicalsourceresolver.XPathResolver;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.Models;
import com.taxonic.carml.util.RdfCollectors;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.util.RmlNamespaces;
import com.taxonic.carml.vocab.Rdf;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

class MappingTester {

  private final RmlMappingLoader loader = RmlMappingLoader.build();

  void testMapping(String contextPath, String rmlPath, String outputPath) {
    testMapping(contextPath, rmlPath, outputPath, m -> {
    }, Map.of());
  }

  void testMapping(String contextPath, String rmlPath, String outputPath,
      Consumer<RdfRmlMapper.Builder> configureMapper) {
    testMapping(contextPath, rmlPath, outputPath, configureMapper, Map.of());
  }

  void testMapping(String contextPath, String rmlPath, String outputPath,
      Consumer<RdfRmlMapper.Builder> configureMapper, Map<String, InputStream> namedInputStreams) {
    Set<TriplesMap> mapping = loader.load(RDFFormat.TURTLE, MappingTester.class.getResourceAsStream(rmlPath));

    RdfRmlMapper.Builder builder = RdfRmlMapper.builder()
        .setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
        .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
        .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
        .classPathResolver(contextPath)
        .triplesMaps(mapping);
    configureMapper.accept(builder);
    RdfRmlMapper mapper = builder.build();

    Model result;
    if (namedInputStreams.isEmpty()) {
      result = mapper.map()
          .collect(RdfCollectors.toRdf4JTreeModel())
          .block();
    } else {
      result = mapper.map(namedInputStreams)
          .collect(RdfCollectors.toRdf4JTreeModel())
          .block();
    }

    // exit for tests without expected output, such as exception tests
    if (outputPath == null) {
      return;
    }

    InputStream expectedModel = MappingTester.class.getResourceAsStream(outputPath);

    Model expected = Models.parse(Objects.requireNonNull(expectedModel), determineRdfFormat(outputPath))
        .stream()
        .collect(RdfCollectors.toRdf4JTreeModel());

    assertThat(result, equalTo(expected));
  }

  RDFFormat determineRdfFormat(String path) {
    return Rio.getParserFormatForFileName(path)
        .orElseThrow(() -> new RuntimeException(String.format("could not determine rdf format from file [%s]", path)));
  }

  static Matcher<Model> equalTo(final Model expected) {
    return new TypeSafeMatcher<>() {

      @Override
      protected boolean matchesSafely(Model actual) {
        return expected.equals(actual);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(String.format("Model with %s statements.", expected.size()));
      }

      @Override
      protected void describeMismatchSafely(final Model item, final Description mismatchDescription) {
        mismatchDescription.appendText(String.format("Model with %s statements.%n%n", item.size()));

        Sets.SetView<Statement> statementsMissing = Sets.difference(expected, item);
        mismatchDescription.appendText(String.format("Statements expected but missing:%n%n"));
        mismatchDescription.appendText(modelToString(new LinkedHashModel(statementsMissing)));

        Sets.SetView<Statement> surplusStatements = Sets.difference(item, expected);
        mismatchDescription.appendText(String.format("Statements that were not expected:%n%n"));
        mismatchDescription.appendText(modelToString(new LinkedHashModel(surplusStatements)));
      }

      private String modelToString(final Model model) {
        RmlNamespaces.applyRmlNameSpaces(model);
        model.setNamespace("ex", "http://example.org/");

        StringWriter stringWriter = new StringWriter();
        Rio.write(model, stringWriter, RDFFormat.TRIG);
        return stringWriter.toString()
            .replace("\r\n", System.lineSeparator())
            .replace("\r", System.lineSeparator());
      }

    };
  }

}
