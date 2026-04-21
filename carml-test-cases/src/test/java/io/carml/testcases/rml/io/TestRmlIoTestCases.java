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

                // Test case bug: Multi-item XPath reference: skills/skill selects multiple nodes; RML spec requires
                // scalar reference values. Nested data should use rml:LogicalView instead.
                "RMLSTC0012e",

                // RMLTTC test-case bug family (same as RMLSTC0004/0007*): expected output has
                // plain string literals "NN" for JSON integer ages, but per RML-IO natural type
                // inference CARML emits "NN"^^xsd:integer (the RMLSTC0001a passing baseline
                // encodes the correct behavior).
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

                // RMLTTC0002k test-case bug: mapping declares no rml:languageMap on the name
                // object map, but default.nq expects "..."@en language-tagged literals. CARML
                // correctly emits plain literals per the mapping.
                "RMLTTC0002k",

                // RMLTTC0002q test-case bug: expected dump2.nq contains xsd:double literals in
                // non-canonical lexical form ("33"^^xsd:double). CARML's ValidatingValueFactory
                // normalizes to the W3C XSD canonical form ("3.3E1"^^xsd:double), which is the
                // spec-correct representation. Fixing this would require either a per-datatype
                // lexical-preservation mode in the value factory or amending the test fixtures.
                "RMLTTC0002q",

                // RMLTTC0006d test-fixture bug: file is named dump1.nq.tar.xz but the actual bytes
                // are gzip-compressed tar (magic 1f 8b …), not xz. Decompression chokes on the
                // declared xz format.
                "RMLTTC0006d");
    }
}
