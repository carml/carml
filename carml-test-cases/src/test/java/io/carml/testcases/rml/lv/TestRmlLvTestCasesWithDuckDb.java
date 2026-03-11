package io.carml.testcases.rml.lv;

import io.carml.testcases.rml.DuckDbTestCaseSuite;
import java.util.List;

/**
 * Runs the RML-LV conformance test suite using the DuckDB evaluator. This verifies that the DuckDB
 * SQL-based logical view evaluator produces the same RDF output as the default reactive evaluator
 * for all DuckDB-compatible test cases (JSON and CSV sources).
 *
 * <p>DuckDB resolves relative file paths from its configured {@code file_search_path}. The
 * {@code classPathResolver()} configured on the mapper builder is automatically propagated to the
 * {@link io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory} via the
 * {@link io.carml.logicalview.FileBasePathConfigurable} interface, so DuckDB can find data files
 * like {@code people.json} or {@code people.csv}.
 *
 * <p>Skipped test cases:
 * <ul>
 *   <li>Mixed-formulation iterable fields — iterable fields with a different reference formulation
 *       than the parent source (e.g. CSV parent with JSONPath iterable)</li>
 * </ul>
 */
class TestRmlLvTestCasesWithDuckDb extends DuckDbTestCaseSuite {

    @Override
    protected String getBasePath() {
        return "/rml/lv/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // Mixed-formulation iterable fields — ExpressionField with nested IterableField using different
                // formulation
                "RMLLVTC0007a", "RMLLVTC0007b", "RMLLVTC0007c");
    }
}
