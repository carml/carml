package io.carml.testcases.rml.io;

import io.carml.testcases.rml.RmlTestCaseSuite;
import java.util.List;

class TestRmlIoTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/io/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // UTF-16 LE BOM JSON source without rml:encoding declared; JSurfer streaming parser
                // does not auto-detect BOM or handle UTF-16, causing a JSON parse failure
                "RMLSTC0001b",

                // CSV natural datatype mapping not yet supported: CsvResolver.getDatatypeMapperFactory()
                // returns Optional.empty(), so integer-looking values remain plain strings instead of
                // being inferred as xsd:integer
                "RMLSTC0004a", // rml:null not declared
                "RMLSTC0004b", // rml:null "" declared
                "RMLSTC0004c", // rml:null "" and rml:null "NULL" declared

                // D2RQ SQL source with placeholder $CONNECTIONDSN; no database container running in
                // this test suite (SQL tests run in TestRmlIoRegistryTestCases with Testcontainers)
                "RMLSTC0006a",

                // rml:CurrentWorkingDirectory resolves against JVM working dir, not classpath; Friends.csv
                // is only on the classpath. Also affected by CSV natural datatype mapping (xsd:integer)
                "RMLSTC0006b",

                // CSV natural datatype mapping not yet supported (same root cause as 0004a/b/c)
                "RMLSTC0007b",

                // Test case bug: default.nq expects xsd:integer but spec prescribes no type inference
                // for unvalidated XML (xs:untypedAtomic). README correctly shows plain literals.
                // See Epic 0 Task 0.17 and Epic N1 Task N1.1 for details.
                "RMLSTC0007c",
                "RMLSTC0007d",

                // CSV natural datatype mapping not yet supported (multi-source; same root cause as 0004a/b/c)
                "RMLSTC0008b",

                // CSV quoted column headers ("id","name","age"): FastCSV strips quotes transparently,
                // so references resolve successfully; spec requires an error for quoted CSV headers
                "RMLSTC0009a",

                // CSV malformed row with fewer fields than header, but mapping only references existing
                // columns; FastCSV lenient mode returns null for missing fields silently.
                // Spec requires an error whenever any row has fewer fields than the header, regardless
                // of which columns the mapping references
                "RMLSTC0010b",

                // XPath parent axis navigation (../@id, ../../@id, ../../../@id): XMLDog builds detached
                // DOM subtrees for matched iterator nodes, so parent axis evaluates to empty sequence
                // when Saxon navigates above the subtree root
                "RMLSTC0012b", // ../@id (parent company id from departments)
                "RMLSTC0012c", // ../../../@id (grandparent company id from employees)
                "RMLSTC0012d", // ../../@id (grandparent company id from department)

                // Target tests (rml:Target) — not yet supported
                "RMLTTC" // all target test cases
                );
    }
}
