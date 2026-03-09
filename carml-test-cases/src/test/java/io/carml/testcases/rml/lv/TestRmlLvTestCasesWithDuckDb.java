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
 * <p>Test cases that use JSON sources with iterators are skipped because DuckDB's
 * {@code read_json_auto} does not support the {@code json_path} parameter in DuckDB 1.x. The
 * {@link DuckDbLogicalViewEvaluatorFactory} still matches these sources (they have a compatible
 * reference formulation), but query execution fails at runtime.
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
        // Skip reason 1: DuckDB's read_json_auto does not support the json_path parameter
        // (DuckDB 1.x). All JSON test cases with iterators fail at runtime because the compiled
        // SQL contains read_json_auto(path, json_path = '$.people[*]'), which is not valid.
        //
        // Skip reason 2: Iterable fields with row index references (rml:iterator + '#' in
        // templates) are not supported by the DuckDB evaluator. The evaluator does not expose
        // the '#' key in its ViewIteration, causing ViewIterationExpressionEvaluationException.
        return List.of(
                // JSON source with iterator $.people[*] (reason 1)
                "RMLLVTC0000a",
                "RMLLVTC0000c",
                "RMLLVTC0001a",
                "RMLLVTC0001b",
                "RMLLVTC0001c",
                "RMLLVTC0001d",
                "RMLLVTC0002a",
                "RMLLVTC0002b",
                "RMLLVTC0002c",
                "RMLLVTC0003a",
                "RMLLVTC0003b",
                "RMLLVTC0003c",
                "RMLLVTC0004a",
                "RMLLVTC0004b",
                "RMLLVTC0004c",
                "RMLLVTC0004d",
                "RMLLVTC0005a",
                "RMLLVTC0005b",
                "RMLLVTC0005c",
                "RMLLVTC0006a",
                "RMLLVTC0006b",
                "RMLLVTC0006c",
                "RMLLVTC0006d",
                "RMLLVTC0006e",
                "RMLLVTC0006f",
                "RMLLVTC0008b",
                "RMLLVTC0008c",
                "RMLLVTC0008d",
                "RMLLVTC0009a",
                "RMLLVTC0009b",
                "RMLLVTC0009c",
                // Iterable fields with '#' row index reference not supported (reason 2)
                "RMLLVTC0007a",
                "RMLLVTC0007b",
                // JSON source with iterator in join parent view (reason 1)
                "RMLLVTC0007c");
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
