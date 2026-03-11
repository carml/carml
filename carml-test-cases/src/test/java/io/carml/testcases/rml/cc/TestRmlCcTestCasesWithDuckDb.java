package io.carml.testcases.rml.cc;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.util.List;
import java.util.Optional;
import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.api.Disabled;

/**
 * DuckDB evaluator conformance tests for RML Containers/Collections.
 *
 * <p>Disabled: RML-CC support (GatherMap, ContainerTermGenerator) is not yet implemented in the
 * DuckDB evaluator. All CC test cases produce container/collection member triples that the DuckDB
 * view evaluator cannot generate.
 *
 * <p>Task: Add RML-CC support to DuckDB evaluator
 */
@Disabled("RML Containers/Collections not yet supported by DuckDB evaluator")
class TestRmlCcTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/cc/test-cases";
    }

    @Override
    protected Optional<IRI> getBaseIri() {
        return Optional.of(iri("http://example.com/base/"));
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // rml:gather with referencing object maps (parentTriplesMap)
                "RMLTC-CC-0008");
    }
}
