package io.carml.testcases.rml.cc;

import static org.eclipse.rdf4j.model.util.Values.iri;

import io.carml.testcases.rml.RmlTestCaseSuite;
import java.util.List;
import java.util.Optional;
import org.eclipse.rdf4j.model.IRI;

class TestRmlCcTestCases extends RmlTestCaseSuite {

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
                "RMLTC-CC-0008" // This will be supported when rml-lv is implemented
                );
    }
}
