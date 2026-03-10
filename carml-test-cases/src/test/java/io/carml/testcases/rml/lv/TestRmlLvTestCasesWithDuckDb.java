package io.carml.testcases.rml.lv;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalview.duckdb.DuckDbLogicalViewEvaluatorFactory;
import io.carml.model.TriplesMap;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.RmlTestCaseSuite;
import io.carml.util.RmlMappingLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs the RML-LV conformance test suite using the DuckDB evaluator. This verifies that the DuckDB
 * SQL-based logical view evaluator produces the same RDF output as the default reactive evaluator
 * for all DuckDB-compatible test cases (JSON and CSV sources).
 *
 * <p>DuckDB resolves relative file paths from its configured {@code file_search_path}. The
 * {@code classPathResolver()} configured on the mapper builder is automatically propagated to the
 * {@link DuckDbLogicalViewEvaluatorFactory} via the {@link io.carml.logicalview.FileBasePathConfigurable}
 * interface, so DuckDB can find data files like {@code people.json} or {@code people.csv}.
 *
 * <p>Skipped test cases:
 * <ul>
 *   <li>Multi-valued expression fields — array-valued JSONPath references (e.g. {@code $.items[*]})
 *       in expression fields are not expanded by the DuckDB evaluator (includes cases combining row
 *       index with multi-valued fields)</li>
 *   <li>Mixed-formulation iterable fields — iterable fields with a different reference formulation
 *       than the parent source (e.g. CSV parent with JSONPath iterable)</li>
 *   <li>View-on-view — not yet supported by the DuckDB evaluator</li>
 *   <li>Type inference — {@code json_extract_string} returns VARCHAR; the evaluator does not cast
 *       to numeric types for typed literal generation</li>
 * </ul>
 */
@Slf4j
class TestRmlLvTestCasesWithDuckDb extends RmlTestCaseSuite {

    private Connection connection;

    @BeforeAll
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    protected String getBasePath() {
        return "/rml/lv/test-cases";
    }

    @Override
    protected List<String> getSkipTests() {
        return List.of(
                // Multi-valued expression fields — array JSONPath references not expanded (includes row index)
                "RMLLVTC0003c",
                "RMLLVTC0004c",
                "RMLLVTC0006a",
                "RMLLVTC0006b",
                "RMLLVTC0006c",
                "RMLLVTC0006d",
                "RMLLVTC0006e",
                "RMLLVTC0006f",
                // Mixed-formulation iterable fields — ExpressionField with nested IterableField using different
                // formulation
                "RMLLVTC0007a",
                "RMLLVTC0007b",
                "RMLLVTC0007c",
                // View-on-view — not yet supported
                "RMLLVTC0000c",
                "RMLLVTC0008a",
                // Type inference — json_extract_string returns VARCHAR, not numeric
                "RMLLVTC0004d");
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var evaluatorFactory = new DuckDbLogicalViewEvaluatorFactory(connection);

        var mapperBuilder = RdfRmlMapper.builder()
                .valueFactorySupplier(ValidatingValueFactory::new)
                .logicalViewEvaluatorFactory(evaluatorFactory);

        Optional.ofNullable(testCase.getDefaultBaseIri()).or(this::getBaseIri).ifPresent(mapperBuilder::baseIri);

        var mappingStream =
                getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument());
        Set<TriplesMap> mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, mappingStream);

        RdfRmlMapper mapper = mapperBuilder
                .triplesMaps(mapping)
                .classPathResolver(ClassPathResolver.of(
                        "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                .build();

        return mapper.map().collect(ModelCollector.toTreeModel()).block();
    }
}
