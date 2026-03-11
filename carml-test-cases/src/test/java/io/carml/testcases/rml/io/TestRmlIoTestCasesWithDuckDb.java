package io.carml.testcases.rml.io;

import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.util.List;

class TestRmlIoTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/io/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // UTF-16 LE BOM JSON source without rml:encoding declared
                "RMLSTC0001b",
                // Test case bugs: xsd:integer for CSV values, empty CSV field handling
                "RMLSTC0004a",
                "RMLSTC0004b",
                "RMLSTC0004c",
                // Test case bug: D2RQ SQL source with no resource.sql
                "RMLSTC0006a",
                // rml:CurrentWorkingDirectory + CSV xsd:integer bug
                "RMLSTC0006b",
                // Test case bug: xsd:integer for CSV values
                "RMLSTC0007b",
                // Test case bug: xsd:integer for unvalidated XML
                "RMLSTC0007c",
                "RMLSTC0007d",
                // Test case bug: xsd:integer for CSV values
                "RMLSTC0008b",
                // Test case bug: quoted CSV headers
                "RMLSTC0009a",
                // Multi-valued JSONPath references: DuckDB returns JSON array as string instead of
                // expanding to individual values. Task: Support multi-valued references in TriplesMap path
                "RMLSTC0011b",
                "RMLSTC0011c",
                "RMLSTC0011e",
                // XML/XPath sources: DuckDB has no native XML support
                "RMLSTC0012a",
                "RMLSTC0012b",
                "RMLSTC0012c",
                "RMLSTC0012d",
                "RMLSTC0012e",
                // Target tests — not yet supported
                "RMLTTC");
    }
}
