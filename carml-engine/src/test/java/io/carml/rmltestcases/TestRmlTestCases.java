package io.carml.rmltestcases;

import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.rmltestcases.model.Dataset;
import io.carml.rmltestcases.model.Input;
import io.carml.rmltestcases.model.Output;
import io.carml.rmltestcases.model.TestCase;
import io.carml.util.Models;
import io.carml.util.RmlMappingLoader;
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
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestRmlTestCases {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    static final IRI EARL_TESTCASE = VF.createIRI("http://www.w3.org/ns/earl#TestCase");

    static final List<String> SUPPORTED_SOURCE_TYPES = ImmutableList.of("CSV", "JSON", "XML");

    // Under discussion in https://github.com/RMLio/rml-test-cases/issues
    private static final List<String> SKIP_TESTS = new ImmutableList.Builder<String>() //
            // https://github.com/kg-construct/rml-test-cases/issues/12
            .add("RMLTC0002c-JSON")
            .add("RMLTC0002c-XML")
            // https://github.com/kg-construct/rml-test-cases/issues/13
            .add("RMLTC0007h-CSV")
            .add("RMLTC0007h-JSON")
            .add("RMLTC0007h-XML")
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
            // https://github.com/kg-construct/rml-test-cases/issues/17
            .add("RMLTC0020b-CSV")
            .add("RMLTC0020b-JSON")
            .add("RMLTC0020b-XML")
            .build();

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

    private Model executeMapping(TestCase testCase) {
        InputStream mappingStream = getDatasetInputStream(testCase.getRules());
        Set<TriplesMap> mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        RdfRmlMapper mapper = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .baseIri(iri("http://example.com/base/"))
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
