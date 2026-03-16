package io.carml.testcases.rml.cc;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.util.Optional;
import org.eclipse.rdf4j.model.IRI;

class TestRmlCcTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/cc/test-cases";
    }

    @Override
    protected Optional<IRI> getBaseIri() {
        return Optional.of(iri("http://example.com/base/"));
    }
}
