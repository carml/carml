package com.taxonic.carml.engine.iotests;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.Sets;
import com.taxonic.carml.engine.RmlMapper;
import com.taxonic.carml.engine.rdf.ModelResult;
import com.taxonic.carml.engine.rdf.RdfRmlMapperBuilder;
import com.taxonic.carml.logical_source_resolver.CsvResolver;
import com.taxonic.carml.logical_source_resolver.JsonPathResolver;
import com.taxonic.carml.logical_source_resolver.XPathResolver;
import com.taxonic.carml.model.TriplesMap;
import com.taxonic.carml.util.IoUtils;
import com.taxonic.carml.util.RmlMappingLoader;
import com.taxonic.carml.util.RmlNamespaces;
import com.taxonic.carml.vocab.Rdf;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

class MappingTest {

  private RmlMappingLoader loader = RmlMappingLoader.build();

  void testMapping(String contextPath, String rmlPath) {
    testMapping(contextPath, rmlPath, null);
  }

  void testMapping(String contextPath, String rmlPath, String outputPath) {
    testMapping(contextPath, rmlPath, outputPath, m -> {
    }, Map.of());
  }

  void testMapping(String contextPath, String rmlPath, String outputPath,
      Consumer<RdfRmlMapperBuilder> configureMapper) {
    testMapping(contextPath, rmlPath, outputPath, configureMapper, Map.of());
  }

  void testMapping(String contextPath, String rmlPath, String outputPath, Consumer<RdfRmlMapperBuilder> configureMapper,
      Map<String, InputStream> namedInputStreams) {
    Set<TriplesMap> mapping = loader.load(RDFFormat.TURTLE, rmlPath);
    RdfRmlMapperBuilder builder =
        new RdfRmlMapperBuilder().setLogicalSourceResolver(Rdf.Ql.Csv, CsvResolver::getInstance)
            .setLogicalSourceResolver(Rdf.Ql.JsonPath, JsonPathResolver::getInstance)
            .setLogicalSourceResolver(Rdf.Ql.XPath, XPathResolver::getInstance)
            .classPathResolver(contextPath)
            .triplesMaps(mapping);
    configureMapper.accept(builder);
    RmlMapper<Statement> mapper = builder.build();
    Model result;
    if (namedInputStreams.isEmpty()) {
      result = ModelResult.from(mapper.map(), TreeModel::new);
    } else {
      result = ModelResult.from(mapper.map(namedInputStreams), TreeModel::new);
    }

    // exit for tests without expected output, such as exception tests
    if (outputPath == null)
      return;

    Model expected = IoUtils.parse(outputPath, determineRdfFormat(outputPath))
        .stream()
        .collect(Collectors.toCollection(TreeModel::new));

    assertThat(result, equalTo(expected));
  }

  RDFFormat determineRdfFormat(String path) {
    return Rio.getParserFormatForFileName(path)
        .orElseThrow(() -> new RuntimeException("could not determine rdf format from file [" + path + "]"));
  }

  public static Matcher<Model> equalTo(final Model expected) {
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
        mismatchDescription.appendText("Model with " + item.size() + " statements.\n\n");

        Sets.SetView<Statement> statementsMissing = Sets.difference(expected, item);
        mismatchDescription.appendText("Statements expected but missing:\n\n");
        mismatchDescription.appendText(modelToString(new LinkedHashModel(statementsMissing)));

        Sets.SetView<Statement> surplusStatements = Sets.difference(item, expected);
        mismatchDescription.appendText("Statements that were not expected:\n\n");
        mismatchDescription.appendText(modelToString(new LinkedHashModel(surplusStatements)));
      }

      private String modelToString(final Model model) {
        RmlNamespaces.applyRmlNameSpaces(model);
        model.setNamespace("ex", "http://example.org/");

        StringWriter stringWriter = new StringWriter();
        Rio.write(model, stringWriter, RDFFormat.TRIG);
        return stringWriter.toString()
            .replace("\r\n", "\n")
            .replace("\r", "\n");
      }

    };
  }

}
