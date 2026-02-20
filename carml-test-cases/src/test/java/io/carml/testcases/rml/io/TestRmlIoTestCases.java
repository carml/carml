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
                "RMLSTC0001b", // JSON BOM parsing error
                "RMLSTC0004a", // CSV natural datatype mapping not yet supported
                "RMLSTC0004b",
                "RMLSTC0004c",
                "RMLSTC0006a", // $CONNECTIONDSN r2dbc connection not available
                "RMLSTC0006b",
                "RMLSTC0007b", // CSV natural datatype mapping not yet supported
                // Test case bug: default.nq expects xsd:integer but spec prescribes no type inference
                // for unvalidated XML (xs:untypedAtomic). README correctly shows plain literals.
                // See Epic 0 Task 0.17 and Epic N1 Task N1.1 for details.
                "RMLSTC0007c",
                "RMLSTC0007d",
                "RMLSTC0008b", // CSV natural datatype mapping not yet supported (multi-source)
                "RMLSTC0009a", // expected error not thrown
                "RMLSTC0010b", // expected error not thrown
                "RMLSTC0012b", // XML isomorphic mismatch
                "RMLSTC0012c",
                "RMLSTC0012d",
                // Target tests - not yet supported
                "RMLTTC" // all target test cases
                );
    }
}
