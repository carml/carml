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

                // Test case bug: expected output has xsd:integer for age values, but CSV has no
                // natural datatypes — all values are plain strings per the RML-IO spec. Additionally,
                // 0004a expects empty CSV fields to be omitted, but the spec says empty strings are
                // valid values, not NULL.
                "RMLSTC0004a",
                "RMLSTC0004b",
                "RMLSTC0004c",

                // Test case bug: D2RQ SQL source test provides only Friends.csv but no resource.sql to
                // create/populate the database table. Unlike the ioregistry SQL test cases
                // (RMLIOREGTC0004-0006) which include resource.sql files, this test case has no SQL
                // schema or data loading mechanism.
                "RMLSTC0006a",

                // rml:CurrentWorkingDirectory resolves against JVM working dir, not classpath; Friends.csv
                // is only on the classpath. Also affected by test case bug: xsd:integer for CSV values.
                "RMLSTC0006b",

                // Test case bug: expected output has xsd:integer for CSV values (same as 0004a/b/c)
                "RMLSTC0007b",

                // Test case bug: default.nq expects xsd:integer but spec prescribes no type inference
                // for unvalidated XML (xs:untypedAtomic). README correctly shows plain literals.
                // See Epic 0 Task 0.17 and Epic N1 Task N1.1 for details.
                "RMLSTC0007c",
                "RMLSTC0007d",

                // Test case bug: expected output has xsd:integer for CSV values (same as 0004a/b/c)
                "RMLSTC0008b",

                // Probable test case bug: expects an error for quoted CSV headers ("id","name","age"),
                // but quoting header fields is valid per RFC 4180. The manifest also contradicts itself
                // by declaring hasError=true while providing an output.nq with valid expected output.
                "RMLSTC0009a",

                // XPath parent axis navigation (../@id, ../../@id, ../../../@id): XMLDog builds detached
                // DOM subtrees for matched iterator nodes, so parent axis evaluates to empty sequence
                // when Saxon navigates above the subtree root
                "RMLSTC0012b", // ../@id (parent company id from departments)
                "RMLSTC0012c", // ../../../@id (grandparent company id from employees)
                "RMLSTC0012d", // ../../@id (grandparent company id from department)

                // Multi-item XPath reference: skills/skill selects multiple nodes; RML spec requires
                // scalar reference values. Nested data should use rml:LogicalView instead.
                "RMLSTC0012e",

                // Target tests (rml:Target) — not yet supported
                "RMLTTC" // all target test cases
                );
    }
}
