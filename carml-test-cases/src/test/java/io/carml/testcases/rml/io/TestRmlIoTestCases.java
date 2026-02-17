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
                // rml:RelativePathSource renamed to rml:FilePath - not yet supported
                "RMLSTC0001a",
                "RMLSTC0001b",
                "RMLSTC0002a",
                "RMLSTC0004a",
                "RMLSTC0004b",
                "RMLSTC0004c",
                "RMLSTC0006a", // $CONNECTIONDSN r2dbc connection not available
                "RMLSTC0006b",
                "RMLSTC0007a",
                "RMLSTC0007b",
                "RMLSTC0007c",
                "RMLSTC0007d",
                "RMLSTC0008a",
                "RMLSTC0008b",
                "RMLSTC0011a",
                "RMLSTC0011b",
                "RMLSTC0011c",
                "RMLSTC0011d",
                "RMLSTC0011e",
                "RMLSTC0012a",
                "RMLSTC0012b",
                "RMLSTC0012c",
                "RMLSTC0012d",
                "RMLSTC0012e",
                // Target tests - not yet supported
                "RMLTTC" // all target test cases
                );
    }
}
