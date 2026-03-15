package io.carml.testcases.rml.core;

import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.util.List;

class TestRmlCoreTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/core/test-cases";
    }

    @Override
    protected List<String> getLenientOutputTests() {
        return List.of("RMLTC0027b");
    }
}
