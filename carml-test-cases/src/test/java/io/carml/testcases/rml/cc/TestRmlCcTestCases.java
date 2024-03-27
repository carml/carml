package io.carml.testcases.rml.cc;

import static org.eclipse.rdf4j.model.util.Models.isomorphic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.CsvResolver;
import io.carml.logicalsourceresolver.JsonPathResolver;
import io.carml.logicalsourceresolver.XPathResolver;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.testcases.model.TestCase;
import io.carml.util.ModelSerializer;
import io.carml.util.Models;
import io.carml.util.RmlMappingLoader;
import io.carml.util.RmlNamespaces;
import java.io.InputStream;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestRmlCcTestCases {

    private static final String BASE_PATH = "/rml/cc/test-cases";

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final IRI TESTCASE = VF.createIRI("http://www.w3.org/2006/03/test-description#TestCase");

    private static final List<String> SUPPORTED_SOURCE_TYPES = List.of("CSV", "JSON", "XML");

    private static final List<String> SKIP_TESTS = List.of("RMLTC-CC-0008"); // This will be supported when rml-lv is
    // implemented

    private RdfRmlMapper.Builder mapperBuilder;

    static Stream<Arguments> populateTestCases() {
        var manifest = TestRmlCcTestCases.class.getResourceAsStream(String.format("%s/manifest.ttl", BASE_PATH));
        return RdfObjectLoader.load(selectTestCases, TestCase.class, Models.parse(manifest, RDFFormat.TURTLE)).stream()
                .filter(TestRmlCcTestCases::shouldBeTested)
                .sorted(Comparator.comparing(TestCase::getIdentifier))
                .map(testCase -> Arguments.of(testCase, testCase.getIdentifier()));
    }

    private static final Function<Model, Set<Resource>> selectTestCases =
            model -> model.filter(null, RDF.TYPE, TESTCASE).subjects().stream().collect(Collectors.toUnmodifiableSet());

    private static boolean isSupported(TestCase testCase) {
        return SUPPORTED_SOURCE_TYPES.contains(testCase.getInput().getInputType());
    }

    private static boolean shouldBeTested(TestCase testCase) {
        return isSupported(testCase)
                && SKIP_TESTS.stream()
                        .noneMatch(skipTest -> testCase.getIdentifier().startsWith(skipTest));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("populateTestCases")
    void runTestCase(TestCase testCase, String testCaseIdentifier) {
        if (!testCase.hasExpectedOutput()) {
            // expect error
            assertThrows(RuntimeException.class, () -> executeMapping(testCase, testCaseIdentifier));
        } else {
            var result = executeMapping(testCase, testCaseIdentifier);

            InputStream expectedOutputStream =
                    getTestCaseFileInputStream(BASE_PATH, testCaseIdentifier, testCase.getOutput());

            Model expected = Models.parse(expectedOutputStream, RDFFormat.NQUADS).stream()
                    .collect(ModelCollector.toTreeModel());

            String resultTtl =
                    ModelSerializer.serializeAsRdf(result, RDFFormat.TURTLE, RmlNamespaces::applyRmlNameSpaces);
            String expectedTtl =
                    ModelSerializer.serializeAsRdf(expected, RDFFormat.TURTLE, RmlNamespaces::applyRmlNameSpaces);
            assertThat(isomorphic(result, expected), is(true));
        }
    }

    private Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalSourceResolverMatcher(CsvResolver.Matcher.getInstance())
                .logicalSourceResolverMatcher(JsonPathResolver.Matcher.getInstance())
                .logicalSourceResolverMatcher(XPathResolver.Matcher.getInstance());

        var mappingStream = getTestCaseFileInputStream(BASE_PATH, testCaseIdentifier, testCase.getMappingDocument());
        Set<TriplesMap> mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        RdfRmlMapper mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        String.format("%s/%s", BASE_PATH, testCase.getIdentifier()), TestRmlCcTestCases.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    private static InputStream getTestCaseFileInputStream(String basePath, String testCaseIdentifier, String fileName) {
        return TestRmlCcTestCases.class.getResourceAsStream(
                String.format("%s/%s/%s", basePath, testCaseIdentifier, fileName));
    }
}
