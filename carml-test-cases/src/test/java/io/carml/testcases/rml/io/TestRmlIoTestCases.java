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
                "RMLSTC0007c", // XML natural datatype mapping not yet supported
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
