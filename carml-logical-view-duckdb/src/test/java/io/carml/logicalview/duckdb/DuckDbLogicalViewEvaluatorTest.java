package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
                        assertThat(first.getKeys(), containsInAnyOrder("name", "age"));
                        assertThat(first.getKeys(), not(containsInAnyOrder(DuckDbViewCompiler.INDEX_COLUMN)));

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
                        assertThat(first.getKeys(), containsInAnyOrder("name", "city"));
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
                        assertThat(iterations.get(0).getIndex(), is(1));
                        assertThat(iterations.get(1).getIndex(), is(2));
                    })
                    .verifyComplete();
        }
    }

    // --- Index column tests ---

    @Nested
    class IndexColumnHandling {

        @Test
        void evaluate_emitsIndexColumn_notInKeysButAccessibleViaGetIndex() throws IOException {
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

                        // getIndex() should return sequential values starting from 1
                        // (ROW_NUMBER() is 1-based)
                        assertThat(iterations.get(0).getIndex(), is(1));
                        assertThat(iterations.get(1).getIndex(), is(2));
                        assertThat(iterations.get(2).getIndex(), is(3));
                    })
                    .verifyComplete();
        }
    }

    // --- ViewIteration contract tests ---

    @Nested
    class ViewIterationContract {

        @Test
        void evaluate_viewIteration_returnsEmptyForReferenceFormulationAndNaturalDatatype() throws IOException {
            var jsonFile = tempDir.resolve("contract.json");
            Files.writeString(jsonFile, """
                    [{"name": "Alice"}]""");

            var view = createJsonView(jsonFile.toString(), null, Set.of(expressionField("name", "name")));
            var evaluator = new DuckDbLogicalViewEvaluator(connection);
            var context = EvaluationContext.defaults();

            StepVerifier.create(evaluator.evaluate(view, source -> null, context))
                    .assertNext(iteration -> {
                        assertThat(iteration.getFieldReferenceFormulation("name"), is(Optional.empty()));
                        assertThat(iteration.getNaturalDatatype("name"), is(Optional.empty()));
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

    // --- Helper methods ---

    private static ExpressionField expressionField(String fieldName, String reference) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(reference);
        return field;
    }

    private static LogicalView createJsonView(String filePath, String iterator, Set<ExpressionField> fields) {
        return createViewWithRefFormulation(Rdf.Ql.JsonPath, filePath, iterator, fields);
    }

    private static LogicalView createCsvView(String filePath, Set<ExpressionField> fields) {
        return createViewWithRefFormulation(Rdf.Ql.Csv, filePath, null, fields);
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
