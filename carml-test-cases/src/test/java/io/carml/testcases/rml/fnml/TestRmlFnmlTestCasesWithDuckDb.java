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

    /**
     * Skip list for the rml-fnml conformance suite when running through the DuckDB evaluator,
     * audited fresh against the upstream test cases as of the 2026-04-20 sync. The DuckDB evaluator
     * routes the CSV source through DuckDB's CSV scanner but reuses the same FnO function-execution
     * path as the reactive evaluator, so failures stem from the same function-descriptor and
     * Java-binding gaps documented in {@link TestRmlFnmlTestCases}. The set of failing tests is
     * identical between the two suites.
     */
    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // ====================================================================
                // grel functions whose FnO signatures (`fno:Function` declarations)
                // are not on the classpath. `grel_java_mapping.ttl` only contains
                // `fno:Mapping` rows; the matching function descriptors are absent.
                // carml-test-cases' `functions.ttl` declares only `grel:toUpperCase`
                // and `grel:string_replace`.
                //   WARN ... no function registered for function IRI [...grel.ttl#<name>]
                // ====================================================================
                "RMLFNMLTC0004-CSV", // grel:string_length
                "RMLFNMLTC0007-CSV", // grel:string_substring
                "RMLFNMLTC0021-CSV", // grel:escape
                "RMLFNMLTC0081-CSV", // grel:string_substring (with language tag)

                // ====================================================================
                // `idlab-fn:toUpperCaseURL` — descriptor and method mapping are both
                // present (idlab-functions-java 1.4.0), but invocation fails:
                //   WARN ... Function execution failed: Failed to invoke function
                //   'https://w3id.org/imec/idlab/function#toUpperCaseURL'
                // Likely a parameter-binding / method-resolution mismatch with
                // CARML's FnO invoker.
                // ====================================================================
                "RMLFNMLTC0003-CSV",
                "RMLFNMLTC0011-CSV",
                "RMLFNMLTC0031-CSV",
                "RMLFNMLTC0061-CSV",

                // ====================================================================
                // Upstream fixture inconsistency — same as the reactive suite. Manifest
                // says `hasError=false` and references `output.nq`, but the upstream
                // 2026-04 sync deleted the file. {@link RmlTestCaseSuite#runTestCase}
                // NPEs in {@code BOMInputStream.builder().setInputStream(null)} when
                // it tries to read the missing expected output.
                // ====================================================================
                "RMLFNMLTC0102-CSV",
                "RMLFNMLTC0103-CSV",
                "RMLFNMLTC0104-CSV");
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
                        requireResource("/fno/functions_idlab_classes_java_mapping.ttl"), RDFFormat.TURTLE)
                // RMLFNMLTC0001-CSV references idlab-fn:alwaysReturnsABC, a synthetic helper that
                // has no descriptor in any classpath TTL — register it programmatically.
                .function("https://w3id.org/imec/idlab/function#alwaysReturnsABC")
                .returns("https://w3id.org/imec/idlab/function#_stringOut", String.class)
                .execute(params -> "ABC");
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
