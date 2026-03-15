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
                // XML/XPath sources: DuckDB has no native XML support
                "RMLIOREGTC0003",
                // Test case bug: expected output has plain literal for JSON integer
                "RMLIOREGTC0007a",
                // Unsupported source types: WoT, SPARQL
                "RMLIOREGTC0008a",
                "RMLIOREGTC0009a",
                "RMLIOREGTC0010a",
                "RMLIOREGTC0011a",
                // CSVW dialect: UTF-16 encoding not supported by DuckDB's read_csv
                "RMLIOREGTC0012f",
                // CSVW quoteChar: case-sensitive column name mismatch (CSV has lowercase
                // headers, mapping references uppercase). DuckDB column names are case-sensitive.
                "RMLIOREGTC0012i");
    }
}
