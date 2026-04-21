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
                // XML/XPath sources: DuckDB has no native XML support
                "RMLSTC0012a",
                "RMLSTC0012b",
                "RMLSTC0012c",
                "RMLSTC0012d",
                "RMLSTC0012e",
                // RMLTTC: same per-test skip list as TestRmlIoTestCases — see that class for
                // rationale. The subset carries over unchanged because none of the skipped
                // behaviors are evaluator-specific (they are either test-case bugs on plain-
                // string JSON ages, or the harness-level parse-as-NQUADS constraint).
                "RMLTTC0001a",
                "RMLTTC0001d",
                "RMLTTC0002a",
                "RMLTTC0002b",
                "RMLTTC0002c",
                "RMLTTC0002d",
                "RMLTTC0002e",
                "RMLTTC0002f",
                "RMLTTC0002g",
                "RMLTTC0002h",
                "RMLTTC0002i",
                "RMLTTC0002j",
                "RMLTTC0002m",
                "RMLTTC0002n",
                "RMLTTC0002o",
                "RMLTTC0002r",
                "RMLTTC0003a",
                "RMLTTC0004a",
                "RMLTTC0004b",
                "RMLTTC0004c",
                "RMLTTC0004d",
                "RMLTTC0004e",
                "RMLTTC0004f",
                "RMLTTC0004g",
                "RMLTTC0005a",
                "RMLTTC0005b",
                "RMLTTC0006a",
                "RMLTTC0006b",
                "RMLTTC0006c",
                "RMLTTC0006e",
                "RMLTTC0007a",
                // RMLTTC0002k: expected @en language tag on names not declared in mapping.
                "RMLTTC0002k",
                // RMLTTC0002q: xsd:double canonical form ("3.3E1" vs "33") mismatch.
                "RMLTTC0002q",
                // RMLTTC0006d test-fixture bug: file named .tar.xz but bytes are gzip-tar.
                "RMLTTC0006d");
    }
}
