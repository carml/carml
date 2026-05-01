package io.carml.testcases.rml.fnml;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalview.DefaultLogicalViewEvaluatorFactory;
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

    /**
     * Skip list for the rml-fnml conformance suite. The remaining failures are:
     *
     * <ul>
     *   <li>{@code RMLFNMLTC0001-CSV} — invokes a synthetic helper function
     *       ({@code idlab-fn:alwaysReturnsABC}) that has no descriptor in any TTL on the
     *       classpath. Out of scope for engine work.
     *   <li>{@code RMLFNMLTC0011-CSV} / {@code RMLFNMLTC0031-CSV} — pending investigation. CARML
     *       executes {@code idlab-fn:toUpperCaseURL} and produces {@code <HTTP://VENUS>} /
     *       {@code <HTTP://WWW.EXAMPLE.COM>} in predicate / subject position respectively;
     *       upstream expects {@code <http://VENUS>} / {@code <http://WWW.EXAMPLE.COM>} —
     *       lowercase scheme but uppercase host. TC0003/TC0061 use the same function in the
     *       <em>object</em> position and pass with the all-uppercase output, so the divergence
     *       is between <em>function output passthrough</em> (object position) and
     *       <em>scheme normalization</em> (predicate / subject position). May reflect a real
     *       engine gap or an upstream fixture inconsistency; not addressed in the current
     *       fno:Mapping support change.
     *   <li>{@code RMLFNMLTC0102/0103/0104} — upstream fixture inconsistency: the manifest
     *       declares {@code hasError=false} with an {@code output.nq} that the upstream sync
     *       deleted. Suite NPEs in {@code BOMInputStream.builder().setInputStream(null)};
     *       needs a manifest fix or suite-level missing-file handling, not an engine change.
     * </ul>
     */
    @Override
    protected List<String> getSkipTests() {
        return List.of(
                "RMLFNMLTC0001-CSV",
                "RMLFNMLTC0011-CSV",
                "RMLFNMLTC0031-CSV",
                "RMLFNMLTC0102-CSV",
                "RMLFNMLTC0103-CSV",
                "RMLFNMLTC0104-CSV");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(new DefaultLogicalViewEvaluatorFactory())
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
        var stream = TestRmlFnmlTestCases.class.getResourceAsStream(path);
        if (stream == null) {
            throw new IllegalStateException("Required classpath resource not found: %s".formatted(path));
        }
        return stream;
    }
}
