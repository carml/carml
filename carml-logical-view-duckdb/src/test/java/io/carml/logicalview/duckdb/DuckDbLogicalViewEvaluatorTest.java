package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.logicalview.DedupStrategy;
import io.carml.logicalview.EvaluationContext;
import io.carml.model.ExpressionField;
import io.carml.model.Field;
import io.carml.model.FileSource;
import io.carml.model.IterableField;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.ReferenceFormulation;
import io.carml.vocab.Rdf;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.test.StepVerifier;

class DuckDbLogicalViewEvaluatorTest {

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

    // --- JSON source tests ---

    @Nested
    class JsonSourceEvaluation {

        @Test
        void evaluate_jsonSource_producesViewIterations() throws IOException {
            var jsonFile = tempDir.resolve("people.json");
            Files.writeString(jsonFile, """
                    [
                        {"name": "Alice", "age": 30},
                        {"name": "Bob", "age": 25}
                    ]""");

            var view = createJsonView(
                    jsonFile.toString(), null, Set.of(expressionField("name", "name"), expressionField("age", "age")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        assertThat(iterations, hasSize(2));

                        var first = iterations.get(0);
                        assertThat(first.getValue("name"), is(Optional.of("Alice")));
                        assertThat(first.getValue("age"), is(Optional.of(30L)));
                        assertThat(first.getKeys(), containsInAnyOrder("#", "name", "name.#", "age", "age.#"));
                        assertThat(first.getKeys(), not(containsInAnyOrder(DuckDbViewCompiler.INDEX_COLUMN)));
                        assertThat(first.getValue("#"), is(Optional.of(0)));

                        var second = iterations.get(1);
                        assertThat(second.getValue("name"), is(Optional.of("Bob")));
                        assertThat(second.getValue("age"), is(Optional.of(25L)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_jsonSourceWithNullValue_returnsEmptyOptional() throws IOException {
            var jsonFile = tempDir.resolve("nulls.json");
            Files.writeString(jsonFile, """
                    [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": null}
                    ]""");

            var view = createJsonView(
                    jsonFile.toString(), null, Set.of(expressionField("id", "id"), expressionField("name", "name")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        assertThat(iterations, hasSize(2));

                        var second = iterations.get(1);
                        assertThat(second.getValue("id"), is(Optional.of(2L)));
                        assertThat(second.getValue("name"), is(Optional.empty()));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_jsonSourceWithMultipleFields_producesCorrectValues() throws IOException {
            var jsonFile = tempDir.resolve("multi_field.json");
            Files.writeString(jsonFile, """
                    [
                        {"id": "x1", "value": "hello", "count": 10},
                        {"id": "x2", "value": "world", "count": 20}
                    ]""");

            var view = createJsonView(
                    jsonFile.toString(),
                    null,
                    Set.of(
                            expressionField("id", "id"),
                            expressionField("value", "value"),
                            expressionField("count", "count")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        assertThat(iterations, hasSize(2));

                        var first = iterations.get(0);
                        assertThat(first.getValue("id"), is(Optional.of("x1")));
                        assertThat(first.getValue("value"), is(Optional.of("hello")));
                        assertThat(first.getValue("count"), is(Optional.of(10L)));

                        var second = iterations.get(1);
                        assertThat(second.getValue("id"), is(Optional.of("x2")));
                        assertThat(second.getValue("value"), is(Optional.of("world")));
                        assertThat(second.getValue("count"), is(Optional.of(20L)));
                    })
                    .verifyComplete();
        }
    }

    // --- CSV source tests ---

    @Nested
    class CsvSourceEvaluation {

        @Test
        void evaluate_csvSource_producesViewIterations() throws IOException {
            var csvFile = tempDir.resolve("data.csv");
            Files.writeString(csvFile, """
                    name,score
                    Alice,95
                    Bob,88
                    Charlie,72""");

            var view = createCsvView(
                    csvFile.toString(), Set.of(expressionField("name", "name"), expressionField("score", "score")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        assertThat(iterations, hasSize(3));

                        assertThat(iterations.get(0).getValue("name"), is(Optional.of("Alice")));
                        assertThat(iterations.get(0).getValue("score"), is(Optional.of(95L)));

                        assertThat(iterations.get(1).getValue("name"), is(Optional.of("Bob")));
                        assertThat(iterations.get(1).getValue("score"), is(Optional.of(88L)));

                        assertThat(iterations.get(2).getValue("name"), is(Optional.of("Charlie")));
                        assertThat(iterations.get(2).getValue("score"), is(Optional.of(72L)));
                    })
                    .verifyComplete();
        }
    }

    // --- Projection tests ---

    @Nested
    class ProjectionEvaluation {

        @Test
        void evaluate_withProjection_selectsOnlyProjectedFields() throws IOException {
            var jsonFile = tempDir.resolve("projection.json");
            Files.writeString(jsonFile, """
                    [
                        {"name": "Alice", "age": 30, "city": "Amsterdam"},
                        {"name": "Bob", "age": 25, "city": "Berlin"}
                    ]""");

            var view = createJsonView(
                    jsonFile.toString(),
                    null,
                    Set.of(
                            expressionField("name", "name"),
                            expressionField("age", "age"),
                            expressionField("city", "city")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.withProjectedFields(Set.of("name", "city"));

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        assertThat(iterations, hasSize(2));

                        var first = iterations.get(0);
                        assertThat(first.getKeys(), containsInAnyOrder("#", "name", "name.#", "city", "city.#"));
                        assertThat(first.getValue("name"), is(Optional.of("Alice")));
                        assertThat(first.getValue("city"), is(Optional.of("Amsterdam")));
                        // "age" should not be present
                        assertThat(first.getValue("age"), is(Optional.empty()));
                    })
                    .verifyComplete();
        }
    }

    // --- Limit tests ---

    @Nested
    class LimitEvaluation {

        @Test
        void evaluate_withLimit_limitsIterations() throws IOException {
            var jsonFile = tempDir.resolve("limit.json");
            Files.writeString(jsonFile, """
                    [
                        {"id": 1},
                        {"id": 2},
                        {"id": 3},
                        {"id": 4},
                        {"id": 5}
                    ]""");

            var view = createJsonView(jsonFile.toString(), null, Set.of(expressionField("id", "id")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.withProjectedFieldsAndLimit(Set.of(), 3L);

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> assertThat(iterations, hasSize(3)))
                    .verifyComplete();
        }
    }

    // --- Dedup tests ---

    @Nested
    class DedupEvaluation {

        @Test
        void evaluate_withDedup_removesDuplicates() throws IOException {
            var jsonFile = tempDir.resolve("dedup.json");
            Files.writeString(jsonFile, """
                    [
                        {"name": "Alice", "role": "admin"},
                        {"name": "Bob", "role": "user"},
                        {"name": "Alice", "role": "admin"},
                        {"name": "Bob", "role": "user"}
                    ]""");

            var view = createJsonView(
                    jsonFile.toString(),
                    null,
                    Set.of(expressionField("name", "name"), expressionField("role", "role")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.of(Set.of(), DedupStrategy.exact(), null);

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        assertThat(iterations, hasSize(2));
                        assertThat(iterations.get(0).getIndex(), is(0));
                        assertThat(iterations.get(1).getIndex(), is(1));
                    })
                    .verifyComplete();
        }
    }

    // --- Index column tests ---

    @Nested
    class IndexColumnHandling {

        @Test
        void evaluate_emitsZeroBasedIndex_notInKeysButAccessibleViaGetIndex() throws IOException {
            var jsonFile = tempDir.resolve("index.json");
            Files.writeString(jsonFile, """
                    [
                        {"name": "Alice"},
                        {"name": "Bob"},
                        {"name": "Charlie"}
                    ]""");

            var view = createJsonView(jsonFile.toString(), null, Set.of(expressionField("name", "name")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        assertThat(iterations, hasSize(3));

                        // __idx should NOT be in the keys
                        for (var iteration : iterations) {
                            assertThat(
                                    "keys should not contain " + DuckDbViewCompiler.INDEX_COLUMN,
                                    iteration.getKeys().contains(DuckDbViewCompiler.INDEX_COLUMN),
                                    is(false));
                        }

                        // getIndex() should return 0-based sequential values
                        // (ROW_NUMBER() is 1-based, converted to 0-based by evaluator)
                        assertThat(iterations.get(0).getIndex(), is(0));
                        assertThat(iterations.get(1).getIndex(), is(1));
                        assertThat(iterations.get(2).getIndex(), is(2));

                        // "#" key should be present in values with 0-based index
                        assertThat(iterations.get(0).getValue("#"), is(Optional.of(0)));
                        assertThat(iterations.get(1).getValue("#"), is(Optional.of(1)));
                        assertThat(iterations.get(2).getValue("#"), is(Optional.of(2)));
                    })
                    .verifyComplete();
        }
    }

    // --- ViewIteration contract tests ---

    @Nested
    class ViewIterationContract {

        @Test
        void evaluate_viewIteration_returnsEmptyForReferenceFormulationAndSourceEvaluation() throws IOException {
            var jsonFile = tempDir.resolve("contract.json");
            Files.writeString(jsonFile, """
                    [{"name": "Alice"}]""");

            var view = createJsonView(jsonFile.toString(), null, Set.of(expressionField("name", "name")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> {
                        assertThat(iteration.getFieldReferenceFormulation("name"), is(Optional.empty()));
                        // read_json_auto source: string fields have no natural datatype
                        assertThat(iteration.getNaturalDatatype("name"), is(Optional.empty()));
                        // Index key always has xsd:integer
                        assertThat(iteration.getNaturalDatatype("#"), is(Optional.of(XSD.INTEGER)));
                        assertThat(iteration.getSourceEvaluation(), is(Optional.empty()));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_viewIteration_returnsEmptyForAbsentKey() throws IOException {
            var jsonFile = tempDir.resolve("absent.json");
            Files.writeString(jsonFile, """
                    [{"name": "Alice"}]""");

            var view = createJsonView(jsonFile.toString(), null, Set.of(expressionField("name", "name")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> assertThat(iteration.getValue("nonexistent"), is(Optional.empty())))
                    .verifyComplete();
        }
    }

    // --- Type inference tests ---

    @Nested
    class TypeInferenceEvaluation {

        @Test
        void evaluate_jsonIteratorWithIntegerField_producesIntegerNaturalDatatype() throws IOException {
            var jsonFile = tempDir.resolve("typed.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "alice", "birth_year": 1995},
                            {"name": "bob", "birth_year": 1999}
                        ]
                    }""");

            var nameField = expressionField("name", "$.name");
            var birthYearField = expressionField("birthyear", "$.birth_year");

            @SuppressWarnings("unchecked")
            var fields = (Set<Field>) (Set<?>) Set.of(nameField, birthYearField);

            var view = createJsonViewWithIterableFields(jsonFile.toString(), "$.people[*]", fields);
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        assertThat(iterations, hasSize(2));

                        var first = iterations.get(0);
                        // String fields should have no natural datatype (VARCHAR maps to nothing)
                        assertThat(first.getNaturalDatatype("name"), is(Optional.empty()));
                        // Integer fields should have xsd:integer
                        assertThat(first.getNaturalDatatype("birthyear"), is(Optional.of(XSD.INTEGER)));
                        // Index key should always have xsd:integer
                        assertThat(first.getNaturalDatatype("#"), is(Optional.of(XSD.INTEGER)));
                        // Ordinal companions should have xsd:integer
                        assertThat(first.getNaturalDatatype("name.#"), is(Optional.of(XSD.INTEGER)));
                        assertThat(first.getNaturalDatatype("birthyear.#"), is(Optional.of(XSD.INTEGER)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_jsonIteratorWithMixedTypes_producesCorrectDatatypes() throws IOException {
            var jsonFile = tempDir.resolve("mixed_types.json");
            Files.writeString(jsonFile, """
                    {
                        "data": [
                            {"label": "test", "count": 42, "ratio": 3.14, "active": true}
                        ]
                    }""");

            var labelField = expressionField("label", "$.label");
            var countField = expressionField("count", "$.count");
            var ratioField = expressionField("ratio", "$.ratio");
            var activeField = expressionField("active", "$.active");

            @SuppressWarnings("unchecked")
            var fields = (Set<Field>) (Set<?>) Set.of(labelField, countField, ratioField, activeField);

            var view = createJsonViewWithIterableFields(jsonFile.toString(), "$.data[*]", fields);
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> {
                        assertThat(iteration.getNaturalDatatype("label"), is(Optional.empty()));
                        assertThat(iteration.getNaturalDatatype("count"), is(Optional.of(XSD.INTEGER)));
                        assertThat(iteration.getNaturalDatatype("ratio"), is(Optional.of(XSD.DOUBLE)));
                        assertThat(iteration.getNaturalDatatype("active"), is(Optional.of(XSD.BOOLEAN)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_csvSource_producesNoNaturalDatatypeForFields() throws IOException {
            var csvFile = tempDir.resolve("typed.csv");
            Files.writeString(csvFile, """
                    name,score
                    Alice,95""");

            var view = createCsvView(
                    csvFile.toString(), Set.of(expressionField("name", "name"), expressionField("score", "score")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> {
                        // CSV fields have no JSON type info — natural datatype is empty
                        assertThat(iteration.getNaturalDatatype("name"), is(Optional.empty()));
                        assertThat(iteration.getNaturalDatatype("score"), is(Optional.empty()));
                        // Index and ordinal keys always have xsd:integer
                        assertThat(iteration.getNaturalDatatype("#"), is(Optional.of(XSD.INTEGER)));
                        assertThat(iteration.getNaturalDatatype("name.#"), is(Optional.of(XSD.INTEGER)));
                    })
                    .verifyComplete();
        }
    }

    // --- Error handling tests ---

    @Nested
    class ErrorHandling {

        @Test
        void evaluate_invalidSource_emitsError() {
            var view = createJsonView("/nonexistent/path/to/file.json", null, Set.of(expressionField("id", "id")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .expectErrorMatches(e -> e instanceof RuntimeException
                            && e.getMessage().contains("Failed to execute DuckDB query for view"))
                    .verify();
        }
    }

    // --- Reactive wrapping tests ---

    @Nested
    class ReactiveWrapping {

        @Test
        void evaluate_executesOnBoundedElasticScheduler() throws IOException {
            var jsonFile = tempDir.resolve("scheduler.json");
            Files.writeString(jsonFile, """
                    [{"id": 1}]""");

            var view = createJsonView(jsonFile.toString(), null, Set.of(expressionField("id", "id")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            // Verify the flux completes successfully when subscribed on bounded elastic
            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> assertThat(iteration.getValue("id"), is(Optional.of(1L))))
                    .verifyComplete();
        }

        @Test
        void evaluate_cancellation_stopsWithoutError() throws IOException {
            var jsonFile = tempDir.resolve("cancel.json");
            var jsonContent = new StringBuilder("[");
            for (var i = 0; i < 100; i++) {
                if (i > 0) {
                    jsonContent.append(",");
                }
                jsonContent.append("{\"id\": %d}".formatted(i));
            }
            jsonContent.append("]");
            Files.writeString(jsonFile, jsonContent.toString());

            var view = createJsonView(jsonFile.toString(), null, Set.of(expressionField("id", "id")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context), 3)
                    .expectNextCount(3)
                    .thenCancel()
                    .verify();
        }

        @Test
        void evaluate_multipleViewIterationsStream_correctly() throws IOException {
            var jsonFile = tempDir.resolve("stream.json");
            var jsonContent = new StringBuilder("[");
            for (var i = 0; i < 100; i++) {
                if (i > 0) {
                    jsonContent.append(",");
                }
                jsonContent.append("{\"id\": %d}".formatted(i));
            }
            jsonContent.append("]");
            Files.writeString(jsonFile, jsonContent.toString());

            var view = createJsonView(jsonFile.toString(), null, Set.of(expressionField("id", "id")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).count())
                    .assertNext(count -> assertThat(count, is(100L)))
                    .verifyComplete();
        }
    }

    // --- Multi-valued expression field tests ---

    @Nested
    class MultiValuedExpressionFieldEvaluation {

        @Test
        void evaluate_multiValuedExpressionField_producesRowPerElement() throws IOException {
            var jsonFile = tempDir.resolve("multi_valued.json");
            // Both people have 2 items to conclusively verify per-parent ordinal reset
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "items": ["book", "pen"]},
                            {"name": "Bob", "items": ["cup", "mug"]}
                        ]
                    }""");

            var nameField = expressionField("name", "$.name");
            var itemField = expressionField("item", "$.items[*]");

            @SuppressWarnings("unchecked")
            var fields = (Set<Field>) (Set<?>) Set.of(nameField, itemField);

            var view = createJsonViewWithIterableFields(jsonFile.toString(), "$.people[*]", fields);
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // 2 + 2 = 4 rows total (Alice has 2 items, Bob has 2)
                        assertThat(iterations, hasSize(4));

                        var aliceRows = iterations.stream()
                                .filter(it -> Optional.of("Alice").equals(it.getValue("name")))
                                .toList();
                        var bobRows = iterations.stream()
                                .filter(it -> Optional.of("Bob").equals(it.getValue("name")))
                                .toList();

                        assertThat(aliceRows, hasSize(2));
                        assertThat(bobRows, hasSize(2));

                        // Verify item values
                        var aliceItems = aliceRows.stream()
                                .map(it -> it.getValue("item"))
                                .toList();
                        assertThat(aliceItems, containsInAnyOrder(Optional.of("book"), Optional.of("pen")));

                        var bobItems =
                                bobRows.stream().map(it -> it.getValue("item")).toList();
                        assertThat(bobItems, containsInAnyOrder(Optional.of("cup"), Optional.of("mug")));

                        // Verify per-parent ordinal reset: both Alice and Bob should have ordinals {0, 1}
                        var aliceOrdinals = aliceRows.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(aliceOrdinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L)));

                        var bobOrdinals = bobRows.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(bobOrdinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L)));

                        // Single-valued field name should have ordinal 0
                        assertThat(aliceRows.get(0).getValue("name.#"), is(Optional.of(0L)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_multiValuedExpressionFieldWithFilter_producesFilteredRows() throws IOException {
            var jsonFile = tempDir.resolve("multi_valued_filtered.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "items": [
                                {"label": "book", "active": true},
                                {"label": "pen", "active": false},
                                {"label": "lamp", "active": true}
                            ]},
                            {"name": "Bob", "items": [
                                {"label": "cup", "active": false},
                                {"label": "mug", "active": true}
                            ]}
                        ]
                    }""");

            var nameField = expressionField("name", "$.name");
            var itemField = expressionField("item", "$.items[?(@.active==true)]");

            @SuppressWarnings("unchecked")
            var fields = (Set<Field>) (Set<?>) Set.of(nameField, itemField);

            var view = createJsonViewWithIterableFields(jsonFile.toString(), "$.people[*]", fields);
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // Alice has 2 active items (book, lamp), Bob has 1 (mug) => 3 rows total
                        assertThat(iterations, hasSize(3));

                        var aliceRows = iterations.stream()
                                .filter(it -> Optional.of("Alice").equals(it.getValue("name")))
                                .toList();
                        var bobRows = iterations.stream()
                                .filter(it -> Optional.of("Bob").equals(it.getValue("name")))
                                .toList();

                        assertThat(aliceRows, hasSize(2));
                        assertThat(bobRows, hasSize(1));

                        // Verify filtered item values (only active items, no inactive ones)
                        var aliceItemStrings = aliceRows.stream()
                                .map(it -> it.getValue("item").orElse("").toString())
                                .toList();
                        assertThat(aliceItemStrings, hasSize(2));
                        assertThat(aliceItemStrings, everyItem(containsString("\"active\":true")));
                        assertThat(aliceItemStrings, everyItem(not(containsString("\"active\":false"))));

                        // Verify sequential ordinals after filtering (0-based, no gaps)
                        var aliceOrdinals = aliceRows.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(aliceOrdinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L)));

                        var bobOrdinals = bobRows.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(bobOrdinals, containsInAnyOrder(Optional.of(0L)));
                    })
                    .verifyComplete();
        }
    }

    // --- Iterable field tests ---

    @Nested
    class IterableFieldEvaluation {

        @Test
        void evaluate_iterableField_producesPerParentOrdinals() throws IOException {
            var jsonFile = tempDir.resolve("iterable.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "hobbies": [{"value": "reading"}, {"value": "coding"}]},
                            {"name": "Bob", "hobbies": [{"value": "gaming"}, {"value": "cooking"}, {"value": "hiking"}]}
                        ]
                    }""");

            var nestedField = expressionField("hobby", "$.value");
            var iterableField = iterableField("item", "$.hobbies[*]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // 2 + 3 = 5 rows total (cross join of parent rows with nested arrays)
                        assertThat(iterations, hasSize(5));

                        // Group by parent name and verify per-parent ordinals
                        var aliceRows = iterations.stream()
                                .filter(it -> Optional.of("Alice").equals(it.getValue("name")))
                                .toList();
                        var bobRows = iterations.stream()
                                .filter(it -> Optional.of("Bob").equals(it.getValue("name")))
                                .toList();

                        assertThat(aliceRows, hasSize(2));
                        assertThat(bobRows, hasSize(3));

                        // Alice's ordinals should be 0 and 1 (reset per parent)
                        // DuckDB range() returns BIGINT, so ordinals are Long values
                        var aliceOrdinals = aliceRows.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(aliceOrdinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L)));

                        // Bob's ordinals should be 0, 1, and 2 (reset per parent)
                        var bobOrdinals = bobRows.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(bobOrdinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L), Optional.of(2L)));

                        // Verify hobby values are present
                        var aliceHobbies = aliceRows.stream()
                                .map(it -> it.getValue("item.hobby"))
                                .toList();
                        assertThat(aliceHobbies, containsInAnyOrder(Optional.of("reading"), Optional.of("coding")));

                        var bobHobbies = bobRows.stream()
                                .map(it -> it.getValue("item.hobby"))
                                .toList();
                        assertThat(
                                bobHobbies,
                                containsInAnyOrder(
                                        Optional.of("gaming"), Optional.of("cooking"), Optional.of("hiking")));

                        // Verify keys include item.# and item.hobby (plus ordinal companions)
                        assertThat(
                                iterations.get(0).getKeys(),
                                containsInAnyOrder("#", "name", "name.#", "item.hobby", "item.#"));
                    })
                    .verifyComplete();
        }
    }

    // --- Slice selector tests ---

    @Nested
    class SliceSelectorEvaluation {

        @Test
        void evaluate_iterableFieldWithSlice_selectsSlicedElements() throws IOException {
            var jsonFile = tempDir.resolve("slice.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "scores": [10, 20, 30, 40, 50]}
                        ]
                    }""");

            var nestedField = expressionField("score", "$");
            var iterableField = iterableField("item", "$.scores[1:4]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // [1:4] selects indices 1,2,3 -> scores 20,30,40
                        assertThat(iterations, hasSize(3));

                        var scores = iterations.stream()
                                .map(it -> it.getValue("item.score"))
                                .toList();
                        assertThat(scores, containsInAnyOrder(Optional.of("20"), Optional.of("30"), Optional.of("40")));

                        // Ordinals should be 0-based within the sliced result
                        var ordinals = iterations.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(ordinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L), Optional.of(2L)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_iterableFieldWithSliceStartOnly_selectsFromStartToEnd() throws IOException {
            var jsonFile = tempDir.resolve("slice_start.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Bob", "tags": ["x", "y", "z"]}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$.tags[2:]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // [2:] selects from index 2 to end -> "z"
                        assertThat(iterations, hasSize(1));
                        assertThat(iterations.get(0).getValue("item.val"), is(Optional.of("z")));
                        assertThat(iterations.get(0).getValue("item.#"), is(Optional.of(0L)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_iterableFieldWithSliceEndOnly_selectsFromBeginningToEnd() throws IOException {
            var jsonFile = tempDir.resolve("slice_end.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Carol", "items": ["a", "b", "c", "d"]}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$.items[:2]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // [:2] selects indices 0,1 -> "a","b"
                        assertThat(iterations, hasSize(2));

                        var vals = iterations.stream()
                                .map(it -> it.getValue("item.val"))
                                .toList();
                        assertThat(vals, containsInAnyOrder(Optional.of("a"), Optional.of("b")));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_iterableFieldWithSliceStep_selectsEveryNthElement() throws IOException {
            var jsonFile = tempDir.resolve("slice_step.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Dan", "values": ["a", "b", "c", "d", "e", "f"]}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$.values[::2]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // [::2] selects indices 0,2,4 -> "a","c","e"
                        assertThat(iterations, hasSize(3));

                        var vals = iterations.stream()
                                .map(it -> it.getValue("item.val"))
                                .toList();
                        assertThat(vals, containsInAnyOrder(Optional.of("a"), Optional.of("c"), Optional.of("e")));

                        // Ordinals should be 0-based within the step result
                        var ordinals = iterations.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(ordinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L), Optional.of(2L)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_iterableFieldWithSliceBoundsAndStep_selectsCorrectElements() throws IOException {
            var jsonFile = tempDir.resolve("slice_bounds_step.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Eve", "values": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$.values[1:8:3]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // [1:8:3] selects indices 1,4,7 -> 20,50,80
                        assertThat(iterations, hasSize(3));

                        var vals = iterations.stream()
                                .map(it -> it.getValue("item.val"))
                                .toList();
                        assertThat(vals, containsInAnyOrder(Optional.of("20"), Optional.of("50"), Optional.of("80")));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_iterableFieldWithNegativeStartSlice_selectsFromEnd() throws IOException {
            var jsonFile = tempDir.resolve("neg_start_slice.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "items": ["a", "b", "c", "d"]}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$.items[-2:]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // [-2:] selects last 2 elements -> "c","d"
                        assertThat(iterations, hasSize(2));

                        var vals = iterations.stream()
                                .map(it -> it.getValue("item.val"))
                                .toList();
                        assertThat(vals, containsInAnyOrder(Optional.of("c"), Optional.of("d")));

                        var ordinals = iterations.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(ordinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_iterableFieldWithNegativeEndSlice_selectsUpToEnd() throws IOException {
            var jsonFile = tempDir.resolve("neg_end_slice.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Bob", "items": ["a", "b", "c", "d"]}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$.items[:-1]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // [:-1] selects all but last -> "a","b","c"
                        assertThat(iterations, hasSize(3));

                        var vals = iterations.stream()
                                .map(it -> it.getValue("item.val"))
                                .toList();
                        assertThat(vals, containsInAnyOrder(Optional.of("a"), Optional.of("b"), Optional.of("c")));

                        var ordinals = iterations.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(ordinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L), Optional.of(2L)));
                    })
                    .verifyComplete();
        }
    }

    // --- Union selector tests ---

    @Nested
    class UnionSelectorEvaluation {

        @Test
        void evaluate_iterableFieldWithIndexUnion_selectsSpecificIndices() throws IOException {
            var jsonFile = tempDir.resolve("index_union.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "scores": [10, 20, 30, 40, 50]}
                        ]
                    }""");

            var nestedField = expressionField("score", "$");
            var iterableField = iterableField("item", "$.scores[0,2,4]", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // [0,2,4] selects indices 0, 2, 4 -> scores 10, 30, 50
                        assertThat(iterations, hasSize(3));

                        var scores = iterations.stream()
                                .map(it -> it.getValue("item.score"))
                                .toList();
                        assertThat(scores, containsInAnyOrder(Optional.of("10"), Optional.of("30"), Optional.of("50")));

                        // Ordinals should be 0-based within the selected result
                        var ordinals = iterations.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(ordinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L), Optional.of(2L)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_iterableFieldWithNameUnion_selectsSpecificKeys() throws IOException {
            var jsonFile = tempDir.resolve("name_union.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "details": {"age": 30, "city": "Amsterdam", "role": "dev"}}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$.details['age','city']", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // ['age','city'] selects 2 keys -> values 30, "Amsterdam"
                        assertThat(iterations, hasSize(2));

                        var vals = iterations.stream()
                                .map(it -> it.getValue("item.val"))
                                .toList();
                        assertThat(vals, containsInAnyOrder(Optional.of("30"), Optional.of("Amsterdam")));

                        // Ordinals should be 0-based
                        var ordinals = iterations.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(ordinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L)));
                    })
                    .verifyComplete();
        }
    }

    // --- Child wildcard tests ---

    @Nested
    class ChildWildcardEvaluation {

        @Test
        void evaluate_iterableFieldWithChildWildcard_selectsAllObjectValues() throws IOException {
            var jsonFile = tempDir.resolve("wildcard.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "details": {"age": 30, "city": "NYC", "active": true}}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$.details.*", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // $.details.* selects all 3 object values
                        assertThat(iterations, hasSize(3));

                        var vals = iterations.stream()
                                .map(it -> it.getValue("item.val"))
                                .toList();
                        assertThat(
                                vals, containsInAnyOrder(Optional.of("30"), Optional.of("NYC"), Optional.of("true")));

                        var ordinals = iterations.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(ordinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L), Optional.of(2L)));
                    })
                    .verifyComplete();
        }
    }

    // --- Recursive descent tests ---

    @Nested
    class RecursiveDescentEvaluation {

        @Test
        void evaluate_iterableFieldWithDeepScan_selectsAllMatchingNodes() throws IOException {
            var jsonFile = tempDir.resolve("recursive.json");
            Files.writeString(jsonFile, """
                    {
                        "people": [
                            {"name": "Alice", "address": {"city": "NYC", "office": {"city": "Boston"}}}
                        ]
                    }""");

            var nestedField = expressionField("val", "$");
            var iterableField = iterableField("item", "$..city", Set.of(nestedField));
            var topField = expressionField("name", "$.name");

            var view = createJsonViewWithIterableFields(
                    jsonFile.toString(), "$.people[*]", Set.of(topField, iterableField));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(
                            evaluator.evaluate(view, source -> null, context).collectList())
                    .assertNext(iterations -> {
                        // $..city selects "NYC" (at $.address.city) and "Boston" (at $.address.office.city)
                        assertThat(iterations, hasSize(2));

                        var vals = iterations.stream()
                                .map(it -> it.getValue("item.val"))
                                .toList();
                        assertThat(vals, containsInAnyOrder(Optional.of("NYC"), Optional.of("Boston")));

                        var ordinals = iterations.stream()
                                .map(it -> it.getValue("item.#"))
                                .toList();
                        assertThat(ordinals, containsInAnyOrder(Optional.of(0L), Optional.of(1L)));
                    })
                    .verifyComplete();
        }
    }

    // --- SQL source type inference tests ---

    @Nested
    class SqlSourceTypeInference {

        @Test
        void evaluate_sqlSourceWithIntegerColumn_producesIntegerNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_int AS SELECT 42 AS value");
            }

            var view = createSqlView("SELECT value FROM test_int", Set.of(expressionField("value", "value")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> {
                        assertThat(iteration.getValue("value"), is(Optional.of(42)));
                        assertThat(iteration.getNaturalDatatype("value"), is(Optional.of(XSD.INTEGER)));
                    })
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithBooleanColumn_producesBooleanNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_bool AS SELECT true AS flag");
            }

            var view = createSqlView("SELECT flag FROM test_bool", Set.of(expressionField("flag", "flag")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(
                            iteration -> assertThat(iteration.getNaturalDatatype("flag"), is(Optional.of(XSD.BOOLEAN))))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithDateColumn_producesDateNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_date AS SELECT DATE '2023-06-15' AS d");
            }

            var view = createSqlView("SELECT d FROM test_date", Set.of(expressionField("d", "d")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> assertThat(iteration.getNaturalDatatype("d"), is(Optional.of(XSD.DATE))))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithTimestampColumn_producesDateTimeNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_ts AS SELECT TIMESTAMP '2009-10-10 12:12:22' AS ts");
            }

            var view = createSqlView("SELECT ts FROM test_ts", Set.of(expressionField("ts", "ts")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(
                            iteration -> assertThat(iteration.getNaturalDatatype("ts"), is(Optional.of(XSD.DATETIME))))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithDecimalColumn_producesDecimalNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_dec AS SELECT CAST(3.14 AS DECIMAL(10,2)) AS amount");
            }

            var view = createSqlView("SELECT amount FROM test_dec", Set.of(expressionField("amount", "amount")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration ->
                            assertThat(iteration.getNaturalDatatype("amount"), is(Optional.of(XSD.DECIMAL))))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithDoubleColumn_producesDoubleNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_dbl AS SELECT CAST(2.718 AS DOUBLE) AS ratio");
            }

            var view = createSqlView("SELECT ratio FROM test_dbl", Set.of(expressionField("ratio", "ratio")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(
                            iteration -> assertThat(iteration.getNaturalDatatype("ratio"), is(Optional.of(XSD.DOUBLE))))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithVarcharColumn_producesNoNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_vc AS SELECT 'hello' AS name");
            }

            var view = createSqlView("SELECT name FROM test_vc", Set.of(expressionField("name", "name")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> assertThat(iteration.getNaturalDatatype("name"), is(Optional.empty())))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithTimeColumn_producesTimeNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_time AS SELECT TIME '14:30:00' AS t");
            }

            var view = createSqlView("SELECT t FROM test_time", Set.of(expressionField("t", "t")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> assertThat(iteration.getNaturalDatatype("t"), is(Optional.of(XSD.TIME))))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithFloatColumn_producesDoubleNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_flt AS SELECT CAST(3.14 AS FLOAT) AS val");
            }

            var view = createSqlView("SELECT val FROM test_flt", Set.of(expressionField("val", "val")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(
                            iteration -> assertThat(iteration.getNaturalDatatype("val"), is(Optional.of(XSD.DOUBLE))))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithTimestampTzColumn_producesDateTimeNaturalDatatype() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_tstz AS SELECT TIMESTAMPTZ '2023-01-01 12:00:00+00' AS ts");
            }

            var view = createSqlView("SELECT ts FROM test_tstz", Set.of(expressionField("ts", "ts")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(
                            iteration -> assertThat(iteration.getNaturalDatatype("ts"), is(Optional.of(XSD.DATETIME))))
                    .verifyComplete();
        }

        @Test
        void evaluate_sqlSourceWithMixedColumns_producesCorrectDatatypes() throws SQLException {
            try (var stmt = connection.createStatement()) {
                stmt.execute("CREATE OR REPLACE TABLE test_mixed ("
                        + "id INTEGER, name VARCHAR, active BOOLEAN, score DOUBLE, created DATE)");
                stmt.execute("INSERT INTO test_mixed VALUES (1, 'Alice', true, 95.5, DATE '2024-01-15')");
            }

            var view = createSqlView(
                    "SELECT id, name, active, score, created FROM test_mixed",
                    Set.of(
                            expressionField("id", "id"),
                            expressionField("name", "name"),
                            expressionField("active", "active"),
                            expressionField("score", "score"),
                            expressionField("created", "created")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> {
                        assertThat(iteration.getNaturalDatatype("id"), is(Optional.of(XSD.INTEGER)));
                        assertThat(iteration.getNaturalDatatype("name"), is(Optional.empty()));
                        assertThat(iteration.getNaturalDatatype("active"), is(Optional.of(XSD.BOOLEAN)));
                        assertThat(iteration.getNaturalDatatype("score"), is(Optional.of(XSD.DOUBLE)));
                        assertThat(iteration.getNaturalDatatype("created"), is(Optional.of(XSD.DATE)));
                        // Index always has xsd:integer
                        assertThat(iteration.getNaturalDatatype("#"), is(Optional.of(XSD.INTEGER)));
                    })
                    .verifyComplete();
        }
    }

    // --- Helper methods ---

    private static ExpressionField expressionField(String fieldName, String reference) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(reference);
        return field;
    }

    @SuppressWarnings("unchecked")
    private static IterableField iterableField(String fieldName, String iterator, Set<ExpressionField> nestedFields) {
        var field = mock(IterableField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getIterator()).thenReturn(iterator);
        lenient().when(field.getFields()).thenReturn((Set<Field>) (Set<?>) nestedFields);
        return field;
    }

    private static LogicalView createJsonView(String filePath, String iterator, Set<ExpressionField> fields) {
        return createViewWithRefFormulation(Rdf.Ql.JsonPath, filePath, iterator, fields);
    }

    private static LogicalView createJsonViewWithIterableFields(String filePath, String iterator, Set<Field> fields) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn(filePath);

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);
        lenient().when(logicalSource.getIterator()).thenReturn(iterator);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        lenient().when(view.getFields()).thenReturn(fields);
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }

    private static LogicalView createCsvView(String filePath, Set<ExpressionField> fields) {
        return createViewWithRefFormulation(Rdf.Ql.Csv, filePath, null, fields);
    }

    @SuppressWarnings("unchecked")
    private static LogicalView createSqlView(String query, Set<ExpressionField> fields) {
        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.Rdb);

        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getQuery()).thenReturn(query);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        lenient().when(view.getFields()).thenReturn((Set<Field>) (Set<?>) fields);
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }

    @SuppressWarnings("unchecked")
    private static LogicalView createViewWithRefFormulation(
            org.eclipse.rdf4j.model.Resource refIri, String filePath, String iterator, Set<ExpressionField> fields) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn(filePath);

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(refIri);

        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);
        lenient().when(logicalSource.getIterator()).thenReturn(iterator);

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        lenient().when(view.getFields()).thenReturn((Set<Field>) (Set<?>) fields);
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }
}
