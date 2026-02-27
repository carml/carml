package io.carml.testcases.rml.lv;

import io.carml.testcases.rml.RmlTestCaseSuite;
import java.util.List;

class TestRmlLvTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/lv/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // --- Iterable field evaluation: LinkedHashMap cannot be cast to JsonNode ---
                "RMLLVTC0002a", // Iterable field
                "RMLLVTC0002b", // Iterable field with multiple children
                "RMLLVTC0002c", // Nested iterable fields
                "RMLLVTC0003b", // Index key: iterable field
                "RMLLVTC0004b", // Natural datatype: index expression field (iterable)
                // --- Natural datatype resolution not yet producing correct typed literals ---
                "RMLLVTC0004a", // Natural datatype: index #
                "RMLLVTC0004c", // Natural datatype: index iterable field
                "RMLLVTC0004d", // Natural datatype: record expression field
                // --- Join with iterable fields: json_item key not resolved ---
                "RMLLVTC0006a", // Left join (iterable field reference)
                "RMLLVTC0006c", // Two left joins (iterable field reference)
                "RMLLVTC0006f", // Index key of field in join
                // --- Reference formulation changes: model loading issue ---
                "RMLLVTC0007a", // Change reference formulations: CSV including JSON array
                "RMLLVTC0007b", // Change reference formulations: CSV including JSON object
                "RMLLVTC0007c", // Change reference formulations: JSON including CSV
                // --- Name collision detection: expected error not thrown ---
                "RMLLVTC0009a" // Name collision: same-parent fields
                );
    }
}
