package io.carml.testcases.rml.fnml;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.RmlTestCaseSuite;
import io.carml.util.RmlMappingLoader;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;

class TestRmlFnmlTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/fnml/test-cases";
    }

    @Override
    protected Optional<IRI> getBaseIri() {
        return Optional.of(iri("http://example.com/base/"));
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // idlab-fn:random: non-deterministic output AND rml:return not resolved
                "RMLFNMLTC0001",
                // rml:return / rml:functionExecution not resolved as term generator expression
                "RMLFNMLTC0002",
                "RMLFNMLTC0003",
                "RMLFNMLTC0004",
                "RMLFNMLTC0005",
                "RMLFNMLTC0011",
                "RMLFNMLTC0041",
                "RMLFNMLTC0061",
                // rml:return not resolved; additionally require graceful degradation (empty output,
                // not an error) when function/parameter/return IRI is unknown
                "RMLFNMLTC0102",
                "RMLFNMLTC0103",
                "RMLFNMLTC0104",
                // rml:inputValue not supported by CarmlInput mapper
                "RMLFNMLTC0007",
                "RMLFNMLTC0008",
                "RMLFNMLTC0021",
                "RMLFNMLTC0032",
                // nested rml:functionExecution in inputValueMap + rml:inputValue not supported
                "RMLFNMLTC0051",
                "RMLFNMLTC0071",
                "RMLFNMLTC0081",
                // rml:functionExecution on subject map not resolved
                "RMLFNMLTC0031");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .addFunctionDescriptions(requireResource("/grel_java_mapping.ttl"), RDFFormat.TURTLE)
                .addFunctionDescriptions(requireResource("/fno/functions_idlab.ttl"), RDFFormat.TURTLE)
                .addFunctionDescriptions(
                        requireResource("/fno/functions_idlab_classes_java_mapping.ttl"), RDFFormat.TURTLE);
        Optional.ofNullable(testCase.getDefaultBaseIri()).or(this::getBaseIri).ifPresent(mapperBuilder::baseIri);

        var mappingStream =
                getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument());
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        var mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }

    private static InputStream requireResource(String path) {
        var stream = TestRmlFnmlTestCases.class.getResourceAsStream(path);
        if (stream == null) {
            throw new IllegalStateException("Required classpath resource not found: %s".formatted(path));
        }
        return stream;
    }
}
