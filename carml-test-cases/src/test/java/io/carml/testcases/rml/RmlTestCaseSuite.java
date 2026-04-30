package io.carml.testcases.rml;

import static io.carml.testcases.matcher.IsIsomorphic.isIsomorphicTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalview.DefaultLogicalViewEvaluatorFactory;
import io.carml.model.TriplesMap;
import io.carml.rdfmapper.util.RdfObjectLoader;
import io.carml.testcases.model.TestCase;
import io.carml.util.Compressions;
import io.carml.util.Models;
import io.carml.util.RmlMappingLoader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
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

    protected List<String> getSkipTests() {
        return List.of();
    }

    /**
     * Returns test identifier prefixes whose expected output requires lenient URI syntax verification.
     * Needed for {@code rml:UnsafeIRI} test cases that produce IRIs with unencoded characters.
     */
    protected List<String> getLenientOutputTests() {
        return List.of();
    }

    protected Optional<IRI> getBaseIri() {
        return Optional.empty();
    }

    Stream<Arguments> populateTestCases() {
        var manifest = RmlTestCaseSuite.class.getResourceAsStream(String.format("%s/manifest.ttl", getBasePath()));
        assertNotNull(manifest);
        return RdfObjectLoader.load(selectTestCases, TestCase.class, Models.parse(manifest, RDFFormat.TURTLE)).stream()
                .filter(this::shouldBeTested)
                .sorted(Comparator.comparing(TestCase::getIdentifier))
                .map(testCase -> Arguments.of(testCase, testCase.getIdentifier()));
    }

    private boolean isSupported(TestCase testCase) {
        // Manifests since rml-lv 2026-03 carry directory-style Input nodes alongside file-style
        // ones (e.g. <.../input/> with empty inputFormat plus <.../input/people.json> with a
        // mediatype). The directory entries don't represent real input streams; ignore them in
        // the supported-format check.
        return testCase.getInputs().stream()
                .filter(input -> input.getInputFormat() != null
                        && !input.getInputFormat().isEmpty())
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
            assertThrows(
                    RuntimeException.class,
                    () -> executeMapping(testCase, testCaseIdentifier),
                    "[%s] Expected error but mapping succeeded".formatted(testCaseIdentifier));
        } else {
            var result = executeMapping(testCase, testCaseIdentifier);

            // Manifests since rml-lv 2026-03 carry directory-style Output nodes (e.g.
            // <.../output/>) alongside file-style ones; the IriFilenamePropertyHandler reduces
            // both to local names, leaving an empty string for the directory entry. Skip those.
            Model expected = testCase.getOutputs().stream()
                    .filter(output -> output != null && !output.isEmpty())
                    .flatMap(output -> parseExpectedOutput(
                            getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, output),
                            output,
                            testCaseIdentifier)
                            .stream())
                    .collect(ModelCollector.toTreeModel());

            assertThat("[%s]".formatted(testCaseIdentifier), result, isIsomorphicTo(expected));
        }
    }

    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory());
        Optional.ofNullable(testCase.getDefaultBaseIri()).or(this::getBaseIri).ifPresent(mapperBuilder::baseIri);

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

    private Model parseExpectedOutput(InputStream stream, String fileName, String testCaseIdentifier) {
        var compression = compressionIriFor(fileName);
        var decodedName = stripCompressionSuffix(fileName);
        var format = detectFormat(decodedName);
        var config = lenientParserConfig(testCaseIdentifier);
        try (var decompressed = Compressions.decompress(stream, compression);
                var bom = BOMInputStream.builder()
                        .setInputStream(decompressed)
                        .setInclude(false)
                        .setByteOrderMarks(ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE)
                        .get()) {
            var bomCharsetName = bom.getBOMCharsetName();
            if (bomCharsetName != null && !"UTF-8".equals(bomCharsetName)) {
                var model = new LinkedHashModel();
                var parser = Rio.createParser(format);
                parser.setParserConfig(config);
                parser.setRDFHandler(new StatementCollector(model));
                try (var reader = new InputStreamReader(bom, Charset.forName(bomCharsetName))) {
                    parser.parse(reader, "");
                }
                return model;
            }
            return Models.parse(bom, format, config);
        } catch (IOException ioException) {
            throw new UncheckedIOException(
                    "Could not read expected output %s for %s".formatted(fileName, testCaseIdentifier), ioException);
        }
    }

    private ParserConfig lenientParserConfig(String testCaseIdentifier) {
        var config = new ParserConfig();
        if (getLenientOutputTests().stream().anyMatch(testCaseIdentifier::startsWith)) {
            config.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
            config.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
        }
        return config;
    }

    private static RDFFormat detectFormat(String fileName) {
        // Non-standard extensions used in RMLTTC fixtures that FileFormat.matchFileName does not
        // recognize — RDF/XML registers "rdf" and RDF/JSON registers "rj".
        if (fileName.endsWith(".rdfxml")) {
            return RDFFormat.RDFXML;
        }
        if (fileName.endsWith(".rdfjson")) {
            return RDFFormat.RDFJSON;
        }
        return FileFormat.matchFileName(
                        fileName, RDFParserRegistry.getInstance().getKeys())
                .orElse(RDFFormat.NQUADS);
    }

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final IRI GZIP = VF.createIRI(RML_NS + "gzip");

    private static final IRI ZIP = VF.createIRI(RML_NS + "zip");

    private static final IRI TARXZ = VF.createIRI(RML_NS + "tarxz");

    private static final IRI TARGZ = VF.createIRI(RML_NS + "targz");

    private static IRI compressionIriFor(String fileName) {
        if (fileName.endsWith(".tar.xz")) {
            return TARXZ;
        }
        if (fileName.endsWith(".tar.gz")) {
            return TARGZ;
        }
        if (fileName.endsWith(".gz")) {
            return GZIP;
        }
        if (fileName.endsWith(".zip")) {
            return ZIP;
        }
        return null;
    }

    private static String stripCompressionSuffix(String fileName) {
        for (var suffix : List.of(".tar.xz", ".tar.gz", ".gz", ".zip")) {
            if (fileName.endsWith(suffix)) {
                return fileName.substring(0, fileName.length() - suffix.length());
            }
        }
        return fileName;
    }
}
