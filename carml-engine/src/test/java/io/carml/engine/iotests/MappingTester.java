package io.carml.engine.iotests;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.Sets;
import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.CsvResolver;
import io.carml.logicalsourceresolver.JsonPathResolver;
import io.carml.logicalsourceresolver.XPathResolver;
import io.carml.model.Mapping;
import io.carml.util.Models;
import io.carml.util.RmlMappingLoader;
import io.carml.util.RmlNamespaces;
import io.carml.vocab.Rdf;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

class MappingTester {

    private final RmlMappingLoader loader = RmlMappingLoader.build();

    void testMapping(String contextPath, String rmlPath, String outputPath) {
        testMapping(contextPath, rmlPath, outputPath, m -> {}, Map.of());
    }

    void testMapping(
            String contextPath, String rmlPath, String outputPath, Consumer<RdfRmlMapper.Builder> configureMapper) {
        testMapping(contextPath, rmlPath, outputPath, configureMapper, Map.of());
    }

    void testMapping(
            String contextPath,
            String rmlPath,
            String outputPath,
            Consumer<RdfRmlMapper.Builder> configureMapper,
            Map<String, InputStream> namedInputStreams) {
        var mapping = Mapping.of(RDFFormat.TURTLE, MappingTester.class.getResourceAsStream(rmlPath));

        RdfRmlMapper.Builder builder = RdfRmlMapper.builder()
                .setLogicalSourceResolverFactory(Rdf.Ql.Csv, CsvResolver.factory())
                .setLogicalSourceResolverFactory(Rdf.Ql.JsonPath, JsonPathResolver.factory())
                .setLogicalSourceResolverFactory(Rdf.Ql.XPath, XPathResolver.factory())
                .classPathResolver(contextPath)
                .mapping(mapping);
        configureMapper.accept(builder);
        RdfRmlMapper mapper = builder.build();

        Model result;
        if (namedInputStreams.isEmpty()) {
            result = mapper.map().collect(ModelCollector.toTreeModel()).block();
        } else {
            result = mapper.map(namedInputStreams)
                    .collect(ModelCollector.toTreeModel())
                    .block();
        }

        // exit for tests without expected output, such as exception tests
        if (outputPath == null) {
            return;
        }

        InputStream expectedModel = MappingTester.class.getResourceAsStream(outputPath);

        Model expected = Models.parse(Objects.requireNonNull(expectedModel), determineRdfFormat(outputPath)).stream()
                .collect(ModelCollector.toTreeModel());

        assertThat(result, equalTo(expected));
    }

    RDFFormat determineRdfFormat(String path) {
        return Rio.getParserFormatForFileName(path)
                .orElseThrow(() ->
                        new RuntimeException(String.format("could not determine RDF format from file [%s]", path)));
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
                return stringWriter
                        .toString()
                        .replace("\r\n", System.lineSeparator())
                        .replace("\r", System.lineSeparator());
            }
        };
    }
}
