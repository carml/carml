package io.carml.testcases.rml.cc;

import io.carml.testcases.rml.RmlTestCaseSuite;
import java.util.List;

class TestRmlCcTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/cc/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                "RMLTC-CC-0003-EB", // Empty bag produces rdf:nil instead of empty rdf:Bag node
                "RMLTC-CC-0008", // This will be supported when rml-lv is implemented
                "RMLTC-CC-0010-Listb" // List blank nodes incorrectly reused across named graphs
                );
    }
}
