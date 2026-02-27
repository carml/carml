package io.carml.testcases.rml.lv;

import io.carml.testcases.rml.RmlTestCaseSuite;
import java.util.List;

class TestRmlLvTestCases extends RmlTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/lv/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // --- Engine does not yet support LogicalView (Epic 3: engine integration) ---
                "RMLLVTC0000a", // Logical view on JSON source
                "RMLLVTC0000b", // Logical view on CSV source
                "RMLLVTC0000c", // Logical view on logical view
                "RMLLVTC0001a", // Expression field: reference
                "RMLLVTC0001b", // Expression field: constant
                "RMLLVTC0001c", // Expression field: template
                "RMLLVTC0001d", // Expression field siblings
                "RMLLVTC0002a", // Iterable field
                "RMLLVTC0002b", // Iterable field with multiple children
                "RMLLVTC0002c", // Nested iterable fields
                "RMLLVTC0003a", // Index key: #
                "RMLLVTC0003b", // Index key: iterable field
                "RMLLVTC0003c", // Index key: expression field
                "RMLLVTC0004a", // Natural datatype: index #
                "RMLLVTC0004b", // Natural datatype: index expression field
                "RMLLVTC0004c", // Natural datatype: index iterable field
                "RMLLVTC0004d", // Natural datatype: record expression field
                "RMLLVTC0006a", // Left join
                "RMLLVTC0006b", // Inner join
                "RMLLVTC0006c", // Two left joins
                "RMLLVTC0006d", // Inner join and left join
                "RMLLVTC0006e", // Two inner joins
                "RMLLVTC0006f", // Index key of field in join
                "RMLLVTC0007a", // Change reference formulations: CSV including JSON array
                "RMLLVTC0007b", // Change reference formulations: CSV including JSON object
                "RMLLVTC0007c", // Change reference formulations: JSON including CSV
                // --- Collision detection implemented, but tests pass for the wrong reason ---
                // These hasError=true tests currently throw "Expected LogicalSource but found
                // CarmlLogicalView" (the LV pipeline isn't wired yet), not our RmlMapperException
                // from validateNoNameCollisions. Skip until the full LV engine pipeline is active.
                "RMLLVTC0009a", // Name collision: same-parent fields
                "RMLLVTC0009b", // Name collision: join field vs base field
                "RMLLVTC0009c" // Name collision: cross-join fields
                );
    }
}
