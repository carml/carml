package io.carml.testcases.rml.ioregistry;

import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.util.List;

class TestRmlIoRegistryTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/ioregistry/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // MySQL/PostgreSQL/SQL Server resolver modules removed (replaced by DuckDB scanners)
                "RMLIOREGTC0004",
                "RMLIOREGTC0005",
                "RMLIOREGTC0006",
                // Test case bugs
                "RMLIOREGTC0002b",
                // DuckDB error handling: invalid JSONPath reference returns NULL instead of error
                "RMLIOREGTC0002c",
                // XML/XPath sources: DuckDB has no native XML support
                "RMLIOREGTC0003",
                // Test case bug: expected output has plain literal for JSON integer
                "RMLIOREGTC0007a",
                // Unsupported source types: WoT, SPARQL
                "RMLIOREGTC0008a",
                "RMLIOREGTC0009a",
                "RMLIOREGTC0010a",
                "RMLIOREGTC0011a",
                // CSVW dialect support not yet implemented in DuckDB evaluator.
                // Task: Support CSVW dialect options (delimiter, null, quoteChar, etc.) in DuckDB
                "RMLIOREGTC0012");
    }
}
