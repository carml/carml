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
                "RMLTC0002a", // Wrong base IRI and missing xsd:integer datatype
                "RMLTC0007c", // Wrong base IRI
                "RMLTC0007d", // Wrong base IRI
                "RMLTC0007e", // Wrong base IRI
                "RMLTC0007f", // Wrong base IRI
                "RMLTC0008a", // Wrong base IRI
                "RMLTC0008b", // Wrong base IRI
                "RMLTC0011b", // Wrong base IRI and missing xsd:integer datatype
                "RMLTC0012a", // Wrong base IRI
                "RMLTC0012d", // CARML supports multi-valued subject maps
                "RMLTC0019a", // Wrong base IRI
                "RMLTC0020a", // Wrong base IRI
                "RMLTC0022c", // Wrong base IRI
                "RMLTC0025a", // Wrong base IRI and missing xsd:integer datatype
                "RMLTC0025b", // Expected error but mapping succeeds
                "RMLTC0025c", // Wrong base IRI and missing xsd:integer datatype
                "RMLTC0026a", // Wrong base IRI and missing xsd:integer datatype
                "RMLTC0026b", // Wrong base IRI and missing xsd:integer datatype
                "RMLTC0026c", // Wrong base IRI and missing xsd:integer datatype
                "RMLTC0026d", // Wrong base IRI and missing xsd:integer datatype
                "RMLTC0027a", // TermType enum mapping error
                "RMLTC0027b" // TermType enum mapping error
                );
    }
}
