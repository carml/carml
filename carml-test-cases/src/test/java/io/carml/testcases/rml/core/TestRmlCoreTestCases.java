package io.carml.testcases.rml.core;

import io.carml.testcases.rml.RmlTestCaseSuite;
import java.util.List;

class TestRmlCoreTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/core/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                "RMLTC0002a", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0007c", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0007d", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0007e", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0007f", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0008a", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0008b", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0011b", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0012a", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0012d", // CARML supports multi-valued subject maps
                "RMLTC0025a", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0025b", // Expected error but mapping succeeds
                "RMLTC0025c", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0026a", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0026b", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0026c", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0026d", // Missing xsd:integer natural datatype (Task 0.3)
                "RMLTC0027a", // TermType enum mapping error (Task 0.2)
                "RMLTC0027b" // TermType enum mapping error (Task 0.2)
                );
    }
}
