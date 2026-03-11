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

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // DuckDB error handling: missing source file produces empty result instead of error
                "RMLTC0002e",
                // DuckDB error handling: invalid iterator syntax produces empty result instead of error
                "RMLTC0002g",
                // JSONPath bracket notation with spaces/special chars not supported by DuckDB json_extract
                // Task: Support bracket notation JSONPath in DuckDB evaluator
                "RMLTC0010a",
                "RMLTC0010b",
                "RMLTC0010c",
                // JSONPath bracket notation with escaped braces
                "RMLTC0023f",
                // DuckDB error handling: invalid subject map produces empty result instead of error
                "RMLTC0025b");
    }
}
