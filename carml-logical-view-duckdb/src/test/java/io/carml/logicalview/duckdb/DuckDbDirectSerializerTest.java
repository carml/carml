package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DuckDbDirectSerializerTest {

    private static final String TEST_TABLE_QUERY = "SELECT * FROM test_table";

    private static Connection connection;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void createAndPopulateTestTable(String tableName, String... insertStatements) throws SQLException {
        try (var stmt = connection.createStatement()) {
            stmt.execute(
                    "CREATE OR REPLACE TABLE %s (%s VARCHAR, %s VARCHAR, %s VARCHAR, %s VARCHAR, %s VARCHAR, %s VARCHAR, %s VARCHAR)"
                            .formatted(
                                    tableName,
                                    DuckDbDirectSerializer.SUBJECT_COL,
                                    DuckDbDirectSerializer.PREDICATE_COL,
                                    DuckDbDirectSerializer.OBJECT_COL,
                                    DuckDbDirectSerializer.OBJECT_TYPE_COL,
                                    DuckDbDirectSerializer.OBJECT_LANG_COL,
                                    DuckDbDirectSerializer.OBJECT_DATATYPE_COL,
                                    DuckDbDirectSerializer.GRAPH_COL));
            for (var insert : insertStatements) {
                stmt.execute(insert);
            }
        }
    }

    private static Model parseNTriples(Path path) throws IOException {
        try (var in = new FileInputStream(path.toFile())) {
            return Rio.parse(in, RDFFormat.NTRIPLES);
        }
    }

    private static Model parseNQuads(Path path) throws IOException {
        try (var in = new FileInputStream(path.toFile())) {
            return Rio.parse(in, RDFFormat.NQUADS);
        }
    }

    @Nested
    class NTriplesFormat {

        @BeforeEach
        void setUpTable() throws SQLException {
            createAndPopulateTestTable(
                    "nt_test",
                    "INSERT INTO nt_test VALUES ('http://example.org/s1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://example.org/Person', 'IRI', NULL, NULL, NULL)",
                    "INSERT INTO nt_test VALUES ('http://example.org/s1', 'http://example.org/name', 'Alice', 'LITERAL', NULL, NULL, NULL)",
                    "INSERT INTO nt_test VALUES ('http://example.org/s1', 'http://example.org/label', 'Alice', 'LITERAL', 'en', NULL, NULL)",
                    "INSERT INTO nt_test VALUES ('http://example.org/s1', 'http://example.org/age', '30', 'LITERAL', NULL, 'http://www.w3.org/2001/XMLSchema#integer', NULL)");
        }

        @Test
        void serialize_iriObject_wrapsInAngleBrackets() throws IOException {
            var outputPath = tempDir.resolve("iri_test.nt");

            var count = DuckDbDirectSerializer.serialize(
                    connection, "SELECT * FROM nt_test WHERE object_type = 'IRI'", "nt", outputPath);

            assertThat(count, is(1L));
            var lines = Files.readAllLines(outputPath);
            assertThat(lines.size(), is(1));
            assertThat(
                    lines.get(0),
                    is(
                            "<http://example.org/s1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> ."));
        }

        static Stream<Arguments> literalVariants() {
            return Stream.of(
                    Arguments.of(
                            "plainLiteral",
                            "SELECT * FROM nt_test WHERE object = 'Alice' AND object_lang IS NULL AND object_datatype IS NULL",
                            "<http://example.org/s1> <http://example.org/name> \"Alice\" ."),
                    Arguments.of(
                            "langTaggedLiteral",
                            "SELECT * FROM nt_test WHERE object_lang IS NOT NULL",
                            "<http://example.org/s1> <http://example.org/label> \"Alice\"@en ."),
                    Arguments.of(
                            "typedLiteral",
                            "SELECT * FROM nt_test WHERE object_datatype IS NOT NULL",
                            "<http://example.org/s1> <http://example.org/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> ."));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("literalVariants")
        void serialize_literalVariant_producesExpectedOutput(String name, String query, String expectedLine)
                throws IOException {
            var outputPath = tempDir.resolve("literal_%s.nt".formatted(name));

            var count = DuckDbDirectSerializer.serialize(connection, query, "nt", outputPath);

            assertThat(count, is(1L));
            var lines = Files.readAllLines(outputPath);
            assertThat(lines.get(0), is(expectedLine));
        }

        @Test
        void serialize_allTypes_producesCorrectLineCount() throws IOException {
            var outputPath = tempDir.resolve("all_types_test.nt");

            var count = DuckDbDirectSerializer.serialize(connection, "SELECT * FROM nt_test", "nt", outputPath);

            assertThat(count, is(4L));
            var lines = Files.readAllLines(outputPath);
            assertThat(lines.size(), is(4));
        }

        @Test
        void serialize_allTypes_parseableByRdf4j() throws IOException {
            var outputPath = tempDir.resolve("all_types_rdf4j.nt");

            DuckDbDirectSerializer.serialize(connection, "SELECT * FROM nt_test", "nt", outputPath);

            var model = parseNTriples(outputPath);
            assertThat(model.size(), is(4));
        }
    }

    @Nested
    class NQuadsFormat {

        @BeforeEach
        void setUpTable() throws SQLException {
            createAndPopulateTestTable(
                    "nq_test",
                    "INSERT INTO nq_test VALUES ('http://example.org/s1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://example.org/Person', 'IRI', NULL, NULL, 'http://example.org/graph1')",
                    "INSERT INTO nq_test VALUES ('http://example.org/s1', 'http://example.org/name', 'Alice', 'LITERAL', NULL, NULL, 'http://example.org/graph1')",
                    "INSERT INTO nq_test VALUES ('http://example.org/s1', 'http://example.org/label', 'Bob', 'LITERAL', NULL, NULL, NULL)");
        }

        @Test
        void serialize_withGraph_includesGraphIri() throws IOException {
            var outputPath = tempDir.resolve("nq_graph_test.nq");

            var count = DuckDbDirectSerializer.serialize(
                    connection,
                    "SELECT * FROM nq_test WHERE graph IS NOT NULL AND object_type = 'IRI'",
                    "nq",
                    outputPath);

            assertThat(count, is(1L));
            var lines = Files.readAllLines(outputPath);
            assertThat(
                    lines.get(0),
                    is(
                            "<http://example.org/s1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Person> <http://example.org/graph1> ."));
        }

        @Test
        void serialize_withNullGraph_omitsGraphField() throws IOException {
            var outputPath = tempDir.resolve("nq_null_graph_test.nq");

            var count = DuckDbDirectSerializer.serialize(
                    connection, "SELECT * FROM nq_test WHERE graph IS NULL", "nq", outputPath);

            assertThat(count, is(1L));
            var lines = Files.readAllLines(outputPath);
            // When graph is NULL in N-Quads, line looks like N-Triple (no graph field)
            assertThat(lines.get(0), is("<http://example.org/s1> <http://example.org/label> \"Bob\" ."));
        }

        @Test
        void serialize_mixedGraphAndNoGraph_producesCorrectOutput() throws IOException {
            var outputPath = tempDir.resolve("nq_mixed_test.nq");

            var count = DuckDbDirectSerializer.serialize(connection, "SELECT * FROM nq_test", "nq", outputPath);

            assertThat(count, is(3L));
            var lines = Files.readAllLines(outputPath);
            assertThat(lines.size(), is(3));
        }

        @Test
        void serialize_allQuads_parseableByRdf4j() throws IOException {
            var outputPath = tempDir.resolve("nq_rdf4j.nq");

            DuckDbDirectSerializer.serialize(connection, "SELECT * FROM nq_test", "nq", outputPath);

            var model = parseNQuads(outputPath);
            assertThat(model.size(), is(3));
        }
    }

    @Nested
    class BlankNodes {

        @BeforeEach
        void setUpTable() throws SQLException {
            createAndPopulateTestTable(
                    "bnode_test",
                    "INSERT INTO bnode_test VALUES ('_:b1', 'http://example.org/name', 'Alice', 'LITERAL', NULL, NULL, NULL)",
                    "INSERT INTO bnode_test VALUES ('http://example.org/s1', 'http://example.org/knows', '_:b2', 'BNODE', NULL, NULL, NULL)");
        }

        @Test
        void serialize_blankNodeSubject_usesUnderscoreColonPrefix() throws IOException {
            var outputPath = tempDir.resolve("bnode_subject_test.nt");

            var count = DuckDbDirectSerializer.serialize(
                    connection, "SELECT * FROM bnode_test WHERE subject LIKE '_:%'", "nt", outputPath);

            assertThat(count, is(1L));
            var lines = Files.readAllLines(outputPath);
            assertThat(lines.get(0), is("_:b1 <http://example.org/name> \"Alice\" ."));
        }

        @Test
        void serialize_blankNodeObject_usesUnderscoreColonPrefix() throws IOException {
            var outputPath = tempDir.resolve("bnode_object_test.nt");

            var count = DuckDbDirectSerializer.serialize(
                    connection, "SELECT * FROM bnode_test WHERE object_type = 'BNODE'", "nt", outputPath);

            assertThat(count, is(1L));
            var lines = Files.readAllLines(outputPath);
            assertThat(lines.get(0), is("<http://example.org/s1> <http://example.org/knows> _:b2 ."));
        }

        @Test
        void serialize_blankNodes_parseableByRdf4j() throws IOException {
            var outputPath = tempDir.resolve("bnode_rdf4j.nt");

            DuckDbDirectSerializer.serialize(connection, "SELECT * FROM bnode_test", "nt", outputPath);

            var model = parseNTriples(outputPath);
            assertThat(model.size(), is(2));
        }
    }

    @Nested
    class Escaping {

        @BeforeEach
        void setUpTable() throws SQLException {
            createAndPopulateTestTable("escape_test");
        }

        static Stream<Arguments> escapeVariants() {
            return Stream.of(
                    Arguments.of(
                            "quotes",
                            "She said \"hello\"",
                            "<http://example.org/s1> <http://example.org/text> \"She said \\\"hello\\\"\" ."),
                    Arguments.of(
                            "backslash",
                            "C:\\Users\\Alice",
                            "<http://example.org/s1> <http://example.org/text> \"C:\\\\Users\\\\Alice\" ."),
                    Arguments.of(
                            "tab", "col1\tcol2", "<http://example.org/s1> <http://example.org/text> \"col1\\tcol2\" ."),
                    Arguments.of(
                            "carriageReturn",
                            "before\rafter",
                            "<http://example.org/s1> <http://example.org/text> \"before\\rafter\" ."));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("escapeVariants")
        void serialize_escapedLiteral_producesExpectedOutput(String name, String insertValue, String expectedLine)
                throws IOException, SQLException {
            try (var pstmt = connection.prepareStatement(
                    "INSERT INTO escape_test VALUES ('http://example.org/s1', 'http://example.org/text', ?, 'LITERAL', NULL, NULL, NULL)")) {
                pstmt.setString(1, insertValue);
                pstmt.execute();
            }
            try {
                var outputPath = tempDir.resolve("escape_%s.nt".formatted(name));
                DuckDbDirectSerializer.serialize(connection, "SELECT * FROM escape_test", "nt", outputPath);

                var content = Files.readString(outputPath).trim();
                assertThat(content, is(expectedLine));
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DELETE FROM escape_test");
                }
            }
        }

        @Test
        void serialize_newlineInLiteral_escapesAsBackslashN() throws IOException, SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute(
                        "INSERT INTO escape_test VALUES ('http://example.org/s1', 'http://example.org/text', 'line1' || CHR(10) || 'line2', 'LITERAL', NULL, NULL, NULL)");
            }
            try {
                var outputPath = tempDir.resolve("newline_escape_test.nt");
                DuckDbDirectSerializer.serialize(connection, "SELECT * FROM escape_test", "nt", outputPath);

                var content = Files.readString(outputPath);
                assertThat(content.trim(), is("<http://example.org/s1> <http://example.org/text> \"line1\\nline2\" ."));
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DELETE FROM escape_test");
                }
            }
        }

        @Test
        void serialize_multipleEscapeCharacters_escapesAllCorrectly() throws IOException, SQLException {
            try (var stmt = connection.createStatement()) {
                // Stored value: he said "hi\<LF><TAB>here" (backslash, then literal newline, then literal tab)
                stmt.execute(
                        "INSERT INTO escape_test VALUES ('http://example.org/s1', 'http://example.org/text', 'he said \"hi\\' || CHR(10) || CHR(9) || 'here\"', 'LITERAL', NULL, NULL, NULL)");
            }
            try {
                var outputPath = tempDir.resolve("multi_escape_test.nt");
                DuckDbDirectSerializer.serialize(connection, "SELECT * FROM escape_test", "nt", outputPath);

                var content = Files.readString(outputPath);
                assertThat(
                        content.trim(),
                        is("<http://example.org/s1> <http://example.org/text> \"he said \\\"hi\\\\\\n\\there\\\"\" ."));
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DELETE FROM escape_test");
                }
            }
        }

        @Test
        void serialize_escapedLiterals_parseableByRdf4j() throws IOException, SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute(
                        "INSERT INTO escape_test VALUES ('http://example.org/s1', 'http://example.org/q', 'She said \"hello\"', 'LITERAL', NULL, NULL, NULL)");
                stmt.execute(
                        "INSERT INTO escape_test VALUES ('http://example.org/s1', 'http://example.org/p', 'C:\\Users\\Alice', 'LITERAL', NULL, NULL, NULL)");
                stmt.execute(
                        "INSERT INTO escape_test VALUES ('http://example.org/s1', 'http://example.org/t', 'col1' || CHR(9) || 'col2', 'LITERAL', NULL, NULL, NULL)");
            }
            try {
                var outputPath = tempDir.resolve("escape_rdf4j.nt");
                DuckDbDirectSerializer.serialize(connection, "SELECT * FROM escape_test", "nt", outputPath);

                var model = parseNTriples(outputPath);
                assertThat(model.size(), is(3));
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DELETE FROM escape_test");
                }
            }
        }
    }

    @Nested
    class ErrorHandling {

        @Test
        void serialize_invalidFormat_throwsIllegalArgumentException() {
            var outputPath = tempDir.resolve("error_test.ttl");

            var exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> DuckDbDirectSerializer.serialize(connection, "SELECT 1", "turtle", outputPath));

            assertThat(exception.getMessage(), containsString("Unsupported RDF format"));
            assertThat(exception.getMessage(), containsString("turtle"));
        }

        @Test
        void serialize_nullConnection_throwsNullPointerException() {
            var outputPath = tempDir.resolve("null_conn_test.nt");

            assertThrows(
                    NullPointerException.class,
                    () -> DuckDbDirectSerializer.serialize(null, "SELECT 1", "nt", outputPath));
        }

        @Test
        void serialize_nullResultQuery_throwsNullPointerException() {
            var outputPath = tempDir.resolve("null_query_test.nt");

            assertThrows(
                    NullPointerException.class,
                    () -> DuckDbDirectSerializer.serialize(connection, null, "nt", outputPath));
        }

        @Test
        void serialize_nullFormat_throwsNullPointerException() {
            var outputPath = tempDir.resolve("null_format_test.nt");

            assertThrows(
                    NullPointerException.class,
                    () -> DuckDbDirectSerializer.serialize(connection, "SELECT 1", null, outputPath));
        }

        @Test
        void serialize_nullOutputPath_throwsNullPointerException() {
            assertThrows(
                    NullPointerException.class,
                    () -> DuckDbDirectSerializer.serialize(connection, "SELECT 1", "nt", null));
        }

        @Test
        void serialize_invalidQuery_throwsDuckDbDirectSerializationException() {
            var outputPath = tempDir.resolve("invalid_query_test.nt");

            assertThrows(
                    DuckDbDirectSerializationException.class,
                    () -> DuckDbDirectSerializer.serialize(
                            connection, "SELECT * FROM nonexistent_table", "nt", outputPath));
        }
    }

    @Nested
    class EmptyResults {

        @Test
        void serialize_emptyResultSet_writesEmptyFile() throws IOException, SQLException {
            createAndPopulateTestTable("empty_test");
            var outputPath = tempDir.resolve("empty_test.nt");

            var count = DuckDbDirectSerializer.serialize(connection, "SELECT * FROM empty_test", "nt", outputPath);

            assertThat(count, is(0L));
            assertThat(Files.exists(outputPath), is(true));
            var content = Files.readString(outputPath);
            assertThat(content.isEmpty(), is(true));
        }
    }

    @Nested
    class ColumnConstants {

        @Test
        void columnConstants_haveExpectedValues() {
            assertThat(DuckDbDirectSerializer.SUBJECT_COL, is("subject"));
            assertThat(DuckDbDirectSerializer.PREDICATE_COL, is("predicate"));
            assertThat(DuckDbDirectSerializer.OBJECT_COL, is("object"));
            assertThat(DuckDbDirectSerializer.OBJECT_TYPE_COL, is("object_type"));
            assertThat(DuckDbDirectSerializer.OBJECT_LANG_COL, is("object_lang"));
            assertThat(DuckDbDirectSerializer.OBJECT_DATATYPE_COL, is("object_datatype"));
            assertThat(DuckDbDirectSerializer.GRAPH_COL, is("graph"));
        }
    }

    @Nested
    class SubqueryInput {

        @Test
        void serialize_withSubquery_producesCorrectOutput() throws IOException, SQLException {
            createAndPopulateTestTable(
                    "subquery_source",
                    "INSERT INTO subquery_source VALUES ('http://example.org/s1', 'http://example.org/p1', 'http://example.org/o1', 'IRI', NULL, NULL, NULL)",
                    "INSERT INTO subquery_source VALUES ('http://example.org/s2', 'http://example.org/p2', 'value', 'LITERAL', NULL, NULL, NULL)");

            var outputPath = tempDir.resolve("subquery_test.nt");
            var subquery = "SELECT * FROM subquery_source WHERE object_type = 'IRI'";

            var count = DuckDbDirectSerializer.serialize(connection, subquery, "nt", outputPath);

            assertThat(count, is(1L));
            var lines = Files.readAllLines(outputPath);
            assertThat(lines.get(0), is("<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> ."));
        }
    }

    @Nested
    class SqlGeneration {

        @Test
        void buildSerializationSql_ntFormat_containsExpectedExpressions() {
            var sql = DuckDbDirectSerializer.buildSerializationSql(TEST_TABLE_QUERY, "nt");

            assertThat(sql, containsString("REPLACE"));
            assertThat(sql, containsString("CASE WHEN"));
            assertThat(sql, containsString("test_table"));
        }

        @Test
        void buildSerializationSql_nqFormat_containsGraphExpression() {
            var sql = DuckDbDirectSerializer.buildSerializationSql(TEST_TABLE_QUERY, "nq");

            assertThat(sql, containsString("REPLACE"));
            assertThat(sql, containsString("CASE WHEN"));
            assertThat(sql, containsString("\"graph\""));
        }
    }

    @Nested
    class LangTaggedWithDatatype {

        @Test
        void serialize_langTagTakesPrecedenceOverDatatype_whenBothPresent() throws IOException, SQLException {
            // Per RDF spec, when both lang and datatype are present, lang takes precedence
            createAndPopulateTestTable(
                    "lang_dt_test",
                    "INSERT INTO lang_dt_test VALUES ('http://example.org/s1', 'http://example.org/label', 'Hello', 'LITERAL', 'en', 'http://www.w3.org/2001/XMLSchema#string', NULL)");

            var outputPath = tempDir.resolve("lang_dt_test.nt");
            DuckDbDirectSerializer.serialize(connection, "SELECT * FROM lang_dt_test", "nt", outputPath);

            var lines = Files.readAllLines(outputPath);
            // Language tag should take precedence in the CASE expression
            assertThat(lines.get(0), is("<http://example.org/s1> <http://example.org/label> \"Hello\"@en ."));
        }
    }
}
