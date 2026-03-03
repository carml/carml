package io.carml.testcases.rml.io;

import io.carml.engine.rdf.RdfRmlMapper;
import io.carml.logicalsourceresolver.sourceresolver.ClassPathResolver;
import io.carml.logicalsourceresolver.sql.MySqlResolver;
import io.carml.logicalsourceresolver.sql.SqlServerResolver;
import io.carml.logicalsourceresolver.sql.sourceresolver.DatabaseConnectionOptions;
import io.carml.model.TriplesMap;
import io.carml.testcases.model.TestCase;
import io.carml.testcases.rml.RmlTestCaseSuite;
import io.carml.util.RmlMappingLoader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.PostgreSQLR2DBCDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;

class TestRmlIoTestCases extends RmlTestCaseSuite {

    @SuppressWarnings("resource")
    @Container
    private static final PostgreSQLContainer<?> POSTGRESQL =
            new PostgreSQLContainer<>("postgres:latest").withUsername("root");

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

                // Multi-item XPath reference: skills/skill selects multiple nodes; RML spec requires
                // scalar reference values. Nested data should use rml:LogicalView instead.
                "RMLSTC0012e",

                // Target tests (rml:Target) — not yet supported
                "RMLTTC" // all target test cases
                );
    }

    @Override
    protected Model executeMapping(TestCase testCase, String testCaseIdentifier) {
        var mapperBuilder = RdfRmlMapper.builder().valueFactorySupplier(ValidatingValueFactory::new);

        byte[] mappingBytes;
        try {
            mappingBytes = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, testCase.getMappingDocument())
                    .readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var mappingContent = new String(mappingBytes, StandardCharsets.UTF_8);

        if (mappingContent.contains("d2rq:jdbcDSN")) {
            Set<TriplesMap> mapping =
                    RmlMappingLoader.build().load(RDFFormat.TURTLE, new ByteArrayInputStream(mappingBytes));

            var sqlStream = getTestCaseFileInputStream(getBasePath(), testCaseIdentifier, "resource.sql");
            if (sqlStream != null) {
                try (Connection conn = DriverManager.getConnection(
                        POSTGRESQL.getJdbcUrl(), POSTGRESQL.getUsername(), POSTGRESQL.getPassword())) {
                    var sql = new String(sqlStream.readAllBytes(), StandardCharsets.UTF_8);
                    conn.createStatement().execute(sql);
                } catch (SQLException | IOException e) {
                    throw new RuntimeException(e);
                }
            }

            mapperBuilder.excludeLogicalSourceResolver(MySqlResolver.NAME);
            mapperBuilder.excludeLogicalSourceResolver(SqlServerResolver.NAME);
            mapperBuilder.databaseConnectionOptions(
                    DatabaseConnectionOptions.of(PostgreSQLR2DBCDatabaseContainer.getOptions(POSTGRESQL)));

            RdfRmlMapper mapper = mapperBuilder
                    .triplesMaps(mapping)
                    .classPathResolver(ClassPathResolver.of(
                            "%s/%s".formatted(getBasePath(), testCase.getIdentifier()), RmlTestCaseSuite.class))
                    .build();

            return mapper.map().collect(ModelCollector.toTreeModel()).block();
        }

        return super.executeMapping(testCase, testCaseIdentifier);
    }
}
