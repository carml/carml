package io.carml.testcases.rml.core;

import io.carml.testcases.rml.RmlTestCaseSuite;
import java.util.List;

class TestRmlCoreTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/core/test-cases";
    }

    @Override
    protected List<String> getLenientOutputTests() {
        return List.of("RMLTC0027b"); // rml:UnsafeIRI produces IRIs with unencoded characters
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // Spec mandates exactly one rml:subjectMap per TriplesMap (hasError=true), but CARML
                // intentionally supports multiple subject maps and produces output for both
                "RMLTC0012d",

                // rml:reference resolves to a JSON array ($.amounts → [30,40,50]); spec requires an
                // error (hasError=true), but CARML's JsonPathResolver unwraps arrays into individual
                // scalar values, producing multiple triples instead of throwing
                "RMLTC0025b");
    }
}
