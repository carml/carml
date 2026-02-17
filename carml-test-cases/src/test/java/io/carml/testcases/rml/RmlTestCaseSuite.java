package io.carml.testcases.rml;

import static io.carml.testcases.matcher.IsIsomorphic.isIsomorphicTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.testcases.model.TestCase;
import io.carml.util.Models;
import io.carml.util.RmlMappingLoader;
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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class RmlTestCaseSuite {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final IRI TESTCASE = VF.createIRI("http://www.w3.org/2006/03/test-description#TestCase");

    private static final Set<String> SUPPORTED_INPUT_FORMATS =
            Set.of("application/json", "text/csv", "application/xml", "text/xml");

    private static final Function<Model, Set<Resource>> selectTestCases =
            model -> model.filter(null, RDF.TYPE, TESTCASE).subjects().stream().collect(Collectors.toUnmodifiableSet());

    protected abstract String getBasePath();

    protected abstract List<String> getSkipTests();

    Stream<Arguments> populateTestCases() {
        var manifest = RmlTestCaseSuite.class.getResourceAsStream(String.format("%s/manifest.ttl", getBasePath()));
        assertNotNull(manifest);
        return RdfObjectLoader.load(selectTestCases, TestCase.class, Models.parse(manifest, RDFFormat.TURTLE)).stream()
                .filter(this::shouldBeTested)
                .sorted(Comparator.comparing(TestCase::getIdentifier))
                .map(testCase -> Arguments.of(testCase, testCase.getIdentifier()));
    }

    private boolean isSupported(TestCase testCase) {
        return testCase.getInputs().stream()
                .allMatch(input -> SUPPORTED_INPUT_FORMATS.contains(input.getInputFormat()));
    }

    private boolean shouldBeTested(TestCase testCase) {
        return isSupported(testCase)
                && getSkipTests().stream()
                        .noneMatch(skipTest -> testCase.getIdentifier().startsWith(skipTest));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("populateTestCases")
    void runTestCase(TestCase testCase, String testCaseIdentifier) {
        if (testCase.hasError()) {
            // expect error
            assertThrows(RuntimeException.class, () -> executeMapping(testCase, testCaseIdentifier));
        } else {
            var result = executeMapping(testCase, testCaseIdentifier);

            Model expected = testCase.getOutputs().stream()
                    .map(output -> getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, output))
                    .flatMap(stream -> Models.parse(stream, RDFFormat.NQUADS).stream())
                    .collect(ModelCollector.toTreeModel());

            assertThat(result, isIsomorphicTo(expected));
        }
    }

    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var mapperBuilder = RdfRmlMapper.builder().valueFactorySupplier(ValidatingValueFactory::new);

        var mappingStream =
                getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument());
        Set<TriplesMap> mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        RdfRmlMapper mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        String.format("%s/%s", getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    protected static InputStream getTestCaseFileInputStream(
            String basePath, String testCaseIdentifier, String fileName) {
        return RmlTestCaseSuite.class.getResourceAsStream(
                String.format("%s/%s/%s", basePath, testCaseIdentifier, fileName));
    }
}
