package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.logicalview.ConstraintDescriptor;
import io.carml.logicalview.FieldDescriptor;
import io.carml.logicalview.SourceSchema;
import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.LogicalSource;
import io.carml.model.ReferenceFormulation;
import io.carml.vocab.Rdf;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.test.StepVerifier;

class DuckDbSourceIntrospectorTest {

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

    // --- JSON source introspection ---

    @Nested
    class JsonSourceIntrospection {

        @Test
        void introspect_jsonSource_discoversFields() throws IOException {
            var jsonFile = tempDir.resolve("simple.json");
            Files.writeString(jsonFile, """
                    [
                        {"name": "Alice", "age": 30, "active": true},
                        {"name": "Bob", "age": 25, "active": false}
                    ]""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.hasFields(), is(true));
                        assertThat(schema.fields(), hasSize(3));

                        var nameField = findField(schema, "name");
                        assertThat(nameField.type(), is("VARCHAR"));
                        assertThat(nameField.iterable(), is(false));
                        assertThat(nameField.hasNestedFields(), is(false));

                        var ageField = findField(schema, "age");
                        assertThat(ageField.type(), is("BIGINT"));

                        var activeField = findField(schema, "active");
                        assertThat(activeField.type(), is("BOOLEAN"));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_jsonSourceWithNulls_detectsNullability() throws IOException {
            var jsonFile = tempDir.resolve("nullable.json");
            Files.writeString(jsonFile, """
                    [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": null}
                    ]""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.hasFields(), is(true));
                        var nameField = findField(schema, "name");
                        // DuckDB infers nullability from the data
                        assertThat(nameField.nullable(), is(true));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_jsonSourceWithDoubleValues_discoversDoubleType() throws IOException {
            var jsonFile = tempDir.resolve("doubles.json");
            Files.writeString(jsonFile, """
                    [
                        {"value": 3.14},
                        {"value": 2.71}
                    ]""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        var valueField = findField(schema, "value");
                        assertThat(valueField.type(), is("DOUBLE"));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_jsonSourceWithIterator_discoversFullSchema() throws IOException {
            // Iterator is ignored during introspection — full source schema is discovered
            var jsonFile = tempDir.resolve("events.json");
            Files.writeString(jsonFile, """
                    {
                        "events": [
                            {"type": "click", "count": 5},
                            {"type": "view", "count": 12}
                        ]
                    }""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), "$.events");
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.fields(), hasSize(1));
                        // Top-level structure is discovered, not the iterator-extracted view
                        var eventsField = findField(schema, "events");
                        assertThat(eventsField.iterable(), is(true));
                        assertThat(eventsField.hasNestedFields(), is(true));
                    })
                    .verifyComplete();
        }
    }

    // --- CSV source introspection ---

    @Nested
    class CsvSourceIntrospection {

        @Test
        void introspect_csvSource_discoversFields() throws IOException {
            var csvFile = tempDir.resolve("data.csv");
            Files.writeString(csvFile, """
                    name,score,rating
                    Alice,95,4.5
                    Bob,88,3.8
                    Charlie,72,4.2""");

            var logicalSource = createCsvLogicalSource(csvFile.toString());
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.hasFields(), is(true));
                        assertThat(schema.fields(), hasSize(3));

                        var nameField = findField(schema, "name");
                        assertThat(nameField.type(), is("VARCHAR"));
                        assertThat(nameField.iterable(), is(false));

                        var scoreField = findField(schema, "score");
                        assertThat(scoreField.type(), is("BIGINT"));

                        var ratingField = findField(schema, "rating");
                        assertThat(ratingField.type(), is("DOUBLE"));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_csvSourceWithHeaders_preservesColumnNames() throws IOException {
            var csvFile = tempDir.resolve("headers.csv");
            Files.writeString(csvFile, """
                    first_name,last_name,email
                    John,Doe,john@example.com
                    Jane,Smith,jane@example.com""");

            var logicalSource = createCsvLogicalSource(csvFile.toString());
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.fields(), hasSize(3));
                        assertThat(findField(schema, "first_name").type(), is("VARCHAR"));
                        assertThat(findField(schema, "last_name").type(), is("VARCHAR"));
                        assertThat(findField(schema, "email").type(), is("VARCHAR"));
                    })
                    .verifyComplete();
        }
    }

    // --- Nested JSON introspection ---

    @Nested
    class NestedJsonIntrospection {

        @Test
        void introspect_jsonWithNestedObject_discoversStructFields() throws IOException {
            var jsonFile = tempDir.resolve("nested.json");
            Files.writeString(jsonFile, """
                    [
                        {"name": "Alice", "address": {"city": "Amsterdam", "zip": "1012"}},
                        {"name": "Bob", "address": {"city": "Berlin", "zip": "10115"}}
                    ]""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.fields(), hasSize(2));

                        var nameField = findField(schema, "name");
                        assertThat(nameField.type(), is("VARCHAR"));
                        assertThat(nameField.hasNestedFields(), is(false));

                        var addressField = findField(schema, "address");
                        assertThat(addressField.type(), is("STRUCT"));
                        assertThat(addressField.hasNestedFields(), is(true));
                        assertThat(addressField.nestedFields(), hasSize(2));

                        var cityField = addressField.nestedFields().stream()
                                .filter(f -> f.name().equals("city"))
                                .findFirst()
                                .orElseThrow();
                        assertThat(cityField.type(), is("VARCHAR"));

                        var zipField = addressField.nestedFields().stream()
                                .filter(f -> f.name().equals("zip"))
                                .findFirst()
                                .orElseThrow();
                        assertThat(zipField.type(), is("VARCHAR"));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_jsonWithArray_discoversIterableFields() throws IOException {
            var jsonFile = tempDir.resolve("arrays.json");
            Files.writeString(jsonFile, """
                    [
                        {"name": "Alice", "scores": [95, 88, 72]},
                        {"name": "Bob", "scores": [60, 75]}
                    ]""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.fields(), hasSize(2));

                        var scoresField = findField(schema, "scores");
                        assertThat(scoresField.iterable(), is(true));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_jsonWithArrayOfObjects_discoversIterableStructFields() throws IOException {
            var jsonFile = tempDir.resolve("array_objects.json");
            Files.writeString(jsonFile, """
                    [
                        {"name": "Alice", "items": [{"type": "book", "count": 3}, {"type": "pen", "count": 5}]},
                        {"name": "Bob", "items": [{"type": "pencil", "count": 10}]}
                    ]""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.fields(), hasSize(2));

                        var itemsField = findField(schema, "items");
                        assertThat(itemsField.iterable(), is(true));
                        assertThat(itemsField.hasNestedFields(), is(true));

                        var typeNested = itemsField.nestedFields().stream()
                                .filter(f -> f.name().equals("type"))
                                .findFirst()
                                .orElseThrow();
                        assertThat(typeNested.type(), is("VARCHAR"));

                        var countNested = itemsField.nestedFields().stream()
                                .filter(f -> f.name().equals("count"))
                                .findFirst()
                                .orElseThrow();
                        assertThat(countNested.type(), is("BIGINT"));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_jsonWithDeeplyNestedStruct_discoversRecursively() throws IOException {
            var jsonFile = tempDir.resolve("deep_nested.json");
            Files.writeString(jsonFile, """
                    [
                        {"person": {"name": "Alice", "location": {"city": "Amsterdam", "country": "NL"}}}
                    ]""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.fields(), hasSize(1));

                        var personField = findField(schema, "person");
                        assertThat(personField.type(), is("STRUCT"));
                        assertThat(personField.hasNestedFields(), is(true));

                        var locationField = personField.nestedFields().stream()
                                .filter(f -> f.name().equals("location"))
                                .findFirst()
                                .orElseThrow();
                        assertThat(locationField.type(), is("STRUCT"));
                        assertThat(locationField.hasNestedFields(), is(true));
                        assertThat(locationField.nestedFields(), hasSize(2));
                    })
                    .verifyComplete();
        }
    }

    // --- FilePath source introspection ---

    @Nested
    class FilePathSourceIntrospection {

        @Test
        void introspect_filePathSource_resolvesPath() throws IOException {
            var jsonFile = tempDir.resolve("filepath.json");
            Files.writeString(jsonFile, "[{\"id\": 1}]");

            var filePath = mock(FilePath.class);
            lenient().when(filePath.getPath()).thenReturn(jsonFile.toString());

            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(filePath);

            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.hasFields(), is(true));
                        assertThat(findField(schema, "id").type(), is("BIGINT"));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_fileSourceWithNoUrl_emitsError() {
            var fileSource = mock(FileSource.class);
            lenient().when(fileSource.getUrl()).thenReturn(null);

            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(fileSource);

            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .expectErrorMatches(e -> e instanceof IllegalArgumentException
                            && e.getMessage().contains("FileSource URL is not defined"))
                    .verify();
        }

        @Test
        void introspect_nullSource_emitsError() {
            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getSource()).thenReturn(null);

            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .expectErrorMatches(e -> e instanceof IllegalArgumentException
                            && e.getMessage().contains("no source defined"))
                    .verify();
        }
    }

    // --- Type parsing unit tests ---

    @Nested
    class TypeParsing {

        @Test
        void parseFieldDescriptor_scalarTypes_returnsLeafDescriptor() {
            var varchar = DuckDbSourceIntrospector.parseFieldDescriptor("col", "VARCHAR", true);
            assertThat(varchar.name(), is("col"));
            assertThat(varchar.type(), is("VARCHAR"));
            assertThat(varchar.nullable(), is(true));
            assertThat(varchar.iterable(), is(false));
            assertThat(varchar.hasNestedFields(), is(false));
        }

        @Test
        void parseFieldDescriptor_structType_returnsNestedDescriptor() {
            var struct =
                    DuckDbSourceIntrospector.parseFieldDescriptor("addr", "STRUCT(city VARCHAR, zip INTEGER)", null);
            assertThat(struct.name(), is("addr"));
            assertThat(struct.type(), is("STRUCT"));
            assertThat(struct.iterable(), is(false));
            assertThat(struct.hasNestedFields(), is(true));
            assertThat(struct.nestedFields(), hasSize(2));
        }

        @Test
        void parseFieldDescriptor_structWithQuotedFieldName_unquotesReservedWord() {
            // DuckDB quotes reserved words like "type" in STRUCT definitions
            var struct = DuckDbSourceIntrospector.parseFieldDescriptor(
                    "event", "STRUCT(\"type\" VARCHAR, count BIGINT)", null);
            assertThat(struct.type(), is("STRUCT"));
            assertThat(struct.hasNestedFields(), is(true));

            var typeField = struct.nestedFields().stream()
                    .filter(f -> f.name().equals("type"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected field named 'type'"));
            assertThat(typeField.type(), is("VARCHAR"));
        }

        @Test
        void parseFieldDescriptor_listType_bracketSyntax_returnsIterableDescriptor() {
            var list = DuckDbSourceIntrospector.parseFieldDescriptor("tags", "VARCHAR[]", null);
            assertThat(list.name(), is("tags"));
            assertThat(list.iterable(), is(true));
            assertThat(list.hasNestedFields(), is(false));
        }

        @Test
        void parseFieldDescriptor_listType_parenSyntax_returnsIterableDescriptor() {
            var list = DuckDbSourceIntrospector.parseFieldDescriptor("tags", "LIST(VARCHAR)", null);
            assertThat(list.name(), is("tags"));
            assertThat(list.iterable(), is(true));
            assertThat(list.hasNestedFields(), is(false));
        }

        @Test
        void parseFieldDescriptor_listOfStruct_bracketSyntax_returnsIterableWithNestedFields() {
            var list = DuckDbSourceIntrospector.parseFieldDescriptor(
                    "items", "STRUCT(name VARCHAR, count BIGINT)[]", null);
            assertThat(list.name(), is("items"));
            assertThat(list.iterable(), is(true));
            assertThat(list.hasNestedFields(), is(true));
            assertThat(list.nestedFields(), hasSize(2));
        }

        @Test
        void parseFieldDescriptor_listOfStruct_parenSyntax_returnsIterableWithNestedFields() {
            var list = DuckDbSourceIntrospector.parseFieldDescriptor(
                    "items", "LIST(STRUCT(name VARCHAR, count BIGINT))", null);
            assertThat(list.name(), is("items"));
            assertThat(list.iterable(), is(true));
            assertThat(list.hasNestedFields(), is(true));
            assertThat(list.nestedFields(), hasSize(2));
        }

        @Test
        void parseFieldDescriptor_nullType_returnsLeafWithNullType() {
            var field = DuckDbSourceIntrospector.parseFieldDescriptor("unknown", null, null);
            assertThat(field.name(), is("unknown"));
            assertThat(field.getType().isEmpty(), is(true));
            assertThat(field.iterable(), is(false));
        }

        @Test
        void splitTopLevelCommas_simpleFields_splitsCorrectly() {
            var parts = DuckDbSourceIntrospector.splitTopLevelCommas("a INTEGER, b VARCHAR");
            assertThat(parts, hasSize(2));
            assertThat(parts, contains("a INTEGER", " b VARCHAR"));
        }

        @Test
        void splitTopLevelCommas_singleField_returnsSingleElementList() {
            var parts = DuckDbSourceIntrospector.splitTopLevelCommas("a INTEGER");
            assertThat(parts, hasSize(1));
            assertThat(parts.get(0), is("a INTEGER"));
        }

        @Test
        void splitTopLevelCommas_nestedStruct_doesNotSplitInsideParens() {
            var parts = DuckDbSourceIntrospector.splitTopLevelCommas("a INTEGER, b STRUCT(x VARCHAR, y INTEGER)");
            assertThat(parts, hasSize(2));
            assertThat(parts.get(1).trim(), is("b STRUCT(x VARCHAR, y INTEGER)"));
        }

        @Test
        void splitTopLevelCommas_bracketSyntax_doesNotSplitInsideBrackets() {
            var parts = DuckDbSourceIntrospector.splitTopLevelCommas("tags VARCHAR[], id INTEGER");
            assertThat(parts, hasSize(2));
            assertThat(parts.get(0), is("tags VARCHAR[]"));
        }

        @Test
        void parseStructFields_parsesNestedStructCorrectly() {
            var fields = DuckDbSourceIntrospector.parseStructFields("name VARCHAR, info STRUCT(x INTEGER, y INTEGER)");
            assertThat(fields, hasSize(2));
            assertThat(fields.get(0).name(), is("name"));
            assertThat(fields.get(0).type(), is("VARCHAR"));
            assertThat(fields.get(1).name(), is("info"));
            assertThat(fields.get(1).type(), is("STRUCT"));
            assertThat(fields.get(1).hasNestedFields(), is(true));
        }

        @Test
        void parseStructFields_entryWithNoSpace_createsLeafWithNoType() {
            var fields = DuckDbSourceIntrospector.parseStructFields("justAName");
            assertThat(fields, hasSize(1));
            assertThat(fields.get(0).name(), is("justAName"));
            assertThat(fields.get(0).getType().isEmpty(), is(true));
        }
    }

    // --- Constraint discovery tests ---

    @Nested
    class ConstraintDiscovery {

        @Test
        void introspect_jsonSource_reportsNoNotNullConstraints() throws IOException {
            // DuckDB DESCRIBE reports all JSON columns as nullable (null=YES),
            // so no NOT NULL constraints are emitted for JSON sources
            var jsonFile = tempDir.resolve("notnull.json");
            Files.writeString(jsonFile, """
                    [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": "Bob"}
                    ]""");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.hasFields(), is(true));
                        var notNullConstraints = schema.constraints().stream()
                                .filter(c -> c.type() == ConstraintDescriptor.ConstraintType.NOT_NULL)
                                .toList();
                        assertThat(notNullConstraints, is(empty()));
                    })
                    .verifyComplete();
        }

        @Test
        void introspect_dbTableWithPrimaryKey_discoversPkConstraint() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS pk_test (id INTEGER PRIMARY KEY, name VARCHAR)");
                stmt.execute("INSERT INTO pk_test VALUES (1, 'Alice'), (2, 'Bob')");
            }

            try {
                var logicalSource = createDbLogicalSource("pk_test");
                var introspector = new DuckDbSourceIntrospector(connection);

                StepVerifier.create(introspector.introspect(logicalSource, null))
                        .assertNext(schema -> {
                            assertThat(schema.hasFields(), is(true));
                            assertThat(schema.fields(), hasSize(2));

                            assertThat(findField(schema, "id").type(), is("INTEGER"));
                            assertThat(findField(schema, "name").type(), is("VARCHAR"));

                            var pkConstraints = schema.constraints().stream()
                                    .filter(c -> c.type() == ConstraintDescriptor.ConstraintType.PRIMARY_KEY)
                                    .toList();
                            assertThat(pkConstraints, hasSize(1));
                            assertThat(pkConstraints.get(0).columns(), contains("id"));
                        })
                        .verifyComplete();
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS pk_test");
                }
            }
        }

        @Test
        void introspect_dbTableWithUniqueConstraint_discoversUniqueConstraint() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS unique_test (id INTEGER, email VARCHAR UNIQUE)");
                stmt.execute("INSERT INTO unique_test VALUES (1, 'a@b.com'), (2, 'c@d.com')");
            }

            try {
                var logicalSource = createDbLogicalSource("unique_test");
                var introspector = new DuckDbSourceIntrospector(connection);

                StepVerifier.create(introspector.introspect(logicalSource, null))
                        .assertNext(schema -> {
                            var uniqueConstraints = schema.constraints().stream()
                                    .filter(c -> c.type() == ConstraintDescriptor.ConstraintType.UNIQUE)
                                    .toList();
                            assertThat(uniqueConstraints, hasSize(1));
                            assertThat(uniqueConstraints.get(0).columns(), contains("email"));
                        })
                        .verifyComplete();
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS unique_test");
                }
            }
        }

        @Test
        void introspect_dbTableWithCompositePk_discoversAllColumns() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute(
                        "CREATE TABLE IF NOT EXISTS composite_pk (a INTEGER, b INTEGER, c VARCHAR, PRIMARY KEY (a, b))");
                stmt.execute("INSERT INTO composite_pk VALUES (1, 1, 'x'), (1, 2, 'y')");
            }

            try {
                var logicalSource = createDbLogicalSource("composite_pk");
                var introspector = new DuckDbSourceIntrospector(connection);

                StepVerifier.create(introspector.introspect(logicalSource, null))
                        .assertNext(schema -> {
                            var pkConstraints = schema.constraints().stream()
                                    .filter(c -> c.type() == ConstraintDescriptor.ConstraintType.PRIMARY_KEY)
                                    .toList();
                            assertThat(pkConstraints, hasSize(1));
                            assertThat(pkConstraints.get(0).columns(), hasSize(2));
                            assertThat(pkConstraints.get(0).columns(), contains("a", "b"));
                        })
                        .verifyComplete();
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS composite_pk");
                }
            }
        }

        @Test
        void introspect_dbTableWithPkAndUnique_discoversBothConstraints() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS multi_constraint ("
                        + "id INTEGER PRIMARY KEY, email VARCHAR UNIQUE, name VARCHAR)");
                stmt.execute("INSERT INTO multi_constraint VALUES (1, 'a@b.com', 'Alice')");
            }

            try {
                var logicalSource = createDbLogicalSource("multi_constraint");
                var introspector = new DuckDbSourceIntrospector(connection);

                StepVerifier.create(introspector.introspect(logicalSource, null))
                        .assertNext(schema -> {
                            var pkConstraints = schema.constraints().stream()
                                    .filter(c -> c.type() == ConstraintDescriptor.ConstraintType.PRIMARY_KEY)
                                    .toList();
                            assertThat(pkConstraints, hasSize(1));
                            assertThat(pkConstraints.get(0).columns(), contains("id"));

                            var uniqueConstraints = schema.constraints().stream()
                                    .filter(c -> c.type() == ConstraintDescriptor.ConstraintType.UNIQUE)
                                    .toList();
                            assertThat(uniqueConstraints, hasSize(1));
                            assertThat(uniqueConstraints.get(0).columns(), contains("email"));
                        })
                        .verifyComplete();
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS multi_constraint");
                }
            }
        }

        @Test
        void introspect_dbTableWithNotNullColumn_discoversNotNullConstraint() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS notnull_db (id INTEGER NOT NULL, name VARCHAR)");
                stmt.execute("INSERT INTO notnull_db VALUES (1, 'Alice')");
            }

            try {
                var logicalSource = createDbLogicalSource("notnull_db");
                var introspector = new DuckDbSourceIntrospector(connection);

                StepVerifier.create(introspector.introspect(logicalSource, null))
                        .assertNext(schema -> {
                            var notNullConstraints = schema.constraints().stream()
                                    .filter(c -> c.type() == ConstraintDescriptor.ConstraintType.NOT_NULL)
                                    .toList();
                            assertThat(notNullConstraints, hasSize(1));
                            assertThat(notNullConstraints.get(0).columns(), contains("id"));
                        })
                        .verifyComplete();
            } finally {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS notnull_db");
                }
            }
        }
    }

    // --- Error handling tests ---

    @Nested
    class ErrorHandling {

        @Test
        void introspect_noReferenceFormulation_emitsError() {
            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(null);

            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .expectErrorMatches(e -> e instanceof IllegalArgumentException
                            && e.getMessage().contains("no reference formulation"))
                    .verify();
        }

        @Test
        void introspect_unsupportedRefFormulation_emitsError() {
            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.XPath);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .expectErrorMatches(e -> e instanceof IllegalArgumentException
                            && e.getMessage().contains("Unsupported reference formulation"))
                    .verify();
        }

        @Test
        void introspect_nonexistentFile_emitsError() {
            var logicalSource = createJsonLogicalSource("/nonexistent/path.json", null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .expectError(SQLException.class)
                    .verify();
        }

        @Test
        void introspect_dbSourceWithNoTableName_emitsError() {
            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Rdb);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
            lenient().when(logicalSource.getTableName()).thenReturn(null);
            lenient().when(logicalSource.getSource()).thenReturn(null);

            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .expectErrorMatches(e -> e instanceof IllegalArgumentException
                            && e.getMessage().contains("no table name defined"))
                    .verify();
        }
    }

    // --- Factory tests ---

    @Nested
    class FactoryMatching {

        @Test
        void match_jsonSource_returnsStrongMatch() {
            var logicalSource = createJsonLogicalSource("/dummy.json", null);
            var factory = new DuckDbSourceIntrospectorFactory(connection);

            var match = factory.match(logicalSource);
            assertThat(match.isPresent(), is(true));
            assertThat(match.get().getMatchScore().getScore(), is(2));
        }

        @Test
        void match_csvSource_returnsStrongMatch() {
            var logicalSource = createCsvLogicalSource("/dummy.csv");
            var factory = new DuckDbSourceIntrospectorFactory(connection);

            var match = factory.match(logicalSource);
            assertThat(match.isPresent(), is(true));
            assertThat(match.get().getMatchScore().getScore(), is(2));
        }

        @Test
        void match_dbSource_returnsStrongMatch() {
            var logicalSource = createDbLogicalSource("test_table");
            var factory = new DuckDbSourceIntrospectorFactory(connection);

            var match = factory.match(logicalSource);
            assertThat(match.isPresent(), is(true));
            assertThat(match.get().getMatchScore().getScore(), is(2));
        }

        @Test
        void match_rmlJsonPathSource_returnsStrongMatch() {
            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Rml.JsonPath);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var factory = new DuckDbSourceIntrospectorFactory(connection);
            assertThat(factory.match(logicalSource).isPresent(), is(true));
        }

        @Test
        void match_sql2008TableSource_returnsStrongMatch() {
            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Rml.SQL2008Table);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var factory = new DuckDbSourceIntrospectorFactory(connection);
            assertThat(factory.match(logicalSource).isPresent(), is(true));
        }

        @Test
        void match_xpathSource_returnsEmpty() {
            var refFormulation = mock(ReferenceFormulation.class);
            lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.XPath);

            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);

            var factory = new DuckDbSourceIntrospectorFactory(connection);

            var match = factory.match(logicalSource);
            assertThat(match.isPresent(), is(false));
        }

        @Test
        void match_noRefFormulation_returnsEmpty() {
            var logicalSource = newLogicalSourceMock();
            lenient().when(logicalSource.getReferenceFormulation()).thenReturn(null);

            var factory = new DuckDbSourceIntrospectorFactory(connection);

            var match = factory.match(logicalSource);
            assertThat(match.isPresent(), is(false));
        }

        @Test
        void zeroArgConstructor_createsFactorySuccessfully() {
            try (var factory = new DuckDbSourceIntrospectorFactory()) {
                var logicalSource = createJsonLogicalSource("/dummy.json", null);
                var match = factory.match(logicalSource);
                assertThat(match.isPresent(), is(true));
            }
        }
    }

    // --- Empty source tests ---

    @Nested
    class EmptySource {

        @Test
        void introspect_emptyJsonArray_returnsSchemaWithSingleJsonColumn() throws IOException {
            // DuckDB infers a single "json" column (type JSON) when the array is empty
            // because it cannot infer the schema from zero elements
            var jsonFile = tempDir.resolve("empty.json");
            Files.writeString(jsonFile, "[]");

            var logicalSource = createJsonLogicalSource(jsonFile.toString(), null);
            var introspector = new DuckDbSourceIntrospector(connection);

            StepVerifier.create(introspector.introspect(logicalSource, null))
                    .assertNext(schema -> {
                        assertThat(schema.fields(), hasSize(1));
                        assertThat(schema.constraints(), is(empty()));
                    })
                    .verifyComplete();
        }
    }

    // --- Helper methods ---

    /**
     * Creates a {@link LogicalSource} mock with a lenient stub on
     * {@link LogicalSource#resolveIteratorAsString()} that mirrors the production default-method
     * semantics. Mockito does not execute default interface methods on mocks, so the stub
     * reproduces the lookup chain: declared iterator first, then the formulation's default
     * (derived from its IRI for {@code rml:JSONPath} → {@code "$"} and {@code rml:XPath} →
     * {@code "/"} since mock formulations don't execute their own default methods either).
     * Tests can override either layer with explicit stubs.
     */
    private static LogicalSource newLogicalSourceMock() {
        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.resolveIteratorAsString()).thenAnswer(invocation -> {
            var declared = logicalSource.getIterator();
            if (declared != null && !declared.isBlank()) {
                return Optional.of(declared);
            }
            var formulation = logicalSource.getReferenceFormulation();
            if (formulation == null) {
                return Optional.empty();
            }
            var iri = formulation.getAsResource();
            if (Rdf.Rml.JsonPath.equals(iri) || Rdf.Ql.JsonPath.equals(iri)) {
                return Optional.of("$");
            }
            if (Rdf.Rml.XPath.equals(iri) || Rdf.Ql.XPath.equals(iri)) {
                return Optional.of("/");
            }
            return Optional.empty();
        });
        return logicalSource;
    }

    private static FieldDescriptor findField(SourceSchema schema, String name) {
        return schema.fields().stream()
                .filter(f -> f.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Field not found: " + name));
    }

    private static LogicalSource createJsonLogicalSource(String filePath, String iterator) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn(filePath);

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

        var logicalSource = newLogicalSourceMock();
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);
        lenient().when(logicalSource.getIterator()).thenReturn(iterator);

        return logicalSource;
    }

    private static LogicalSource createCsvLogicalSource(String filePath) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn(filePath);

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Csv);

        var logicalSource = newLogicalSourceMock();
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);

        return logicalSource;
    }

    private static LogicalSource createDbLogicalSource(String tableName) {
        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Rdb);

        var logicalSource = newLogicalSourceMock();
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getTableName()).thenReturn(tableName);

        return logicalSource;
    }
}
