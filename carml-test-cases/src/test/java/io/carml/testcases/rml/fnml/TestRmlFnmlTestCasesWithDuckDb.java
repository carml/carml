package io.carml.testcases.rml.fnml;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.DuckDbTestCaseSuite;
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

class TestRmlFnmlTestCasesWithDuckDb extends DuckDbTestCaseSuite {

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
                // Test case bug: idlab-fn:random
                "RMLFNMLTC0001",
                // Test case bugs: contradictory IRI scheme normalization expectations
                "RMLFNMLTC0011",
                "RMLFNMLTC0031",
                // Test case bug: parameter resource IRI mismatch
                "RMLFNMLTC0003",
                "RMLFNMLTC0061",
                // Test case bug: missing GREL function descriptions
                "RMLFNMLTC0004",
                "RMLFNMLTC0007",
                "RMLFNMLTC0021",
                "RMLFNMLTC0081");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var evaluatorFactory = new DuckDbLogicalViewEvaluatorFactory(getConnection());

        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(evaluatorFactory)
                .addFunctionDescriptions(requireResource("/grel_java_mapping.ttl"), RDFFormat.TURTLE)
                .addFunctionDescriptions(requireResource("/rml/fnml/test-cases/functions.ttl"), RDFFormat.TURTLE)
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
        var stream = TestRmlFnmlTestCasesWithDuckDb.class.getResourceAsStream(path);
        if (stream == null) {
            throw new IllegalStateException("Required classpath resource not found: %s".formatted(path));
        }
        return stream;
    }
}
