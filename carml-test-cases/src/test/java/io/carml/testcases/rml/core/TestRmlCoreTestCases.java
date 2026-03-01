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
        return List.of();
    }
}
