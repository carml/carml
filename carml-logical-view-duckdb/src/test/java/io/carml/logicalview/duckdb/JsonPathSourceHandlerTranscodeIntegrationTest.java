package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.logicalview.EvaluationContext;
import io.carml.model.ExpressionField;
import io.carml.model.FileSource;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.ReferenceFormulation;
import io.carml.vocab.Rdf;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * End-to-end check that wires the {@link JsonNdjsonTranscodeCache} through {@link
 * JsonPathSourceHandler} into a live {@link DuckDbLogicalViewEvaluator}. The threshold is set
 * artificially low so a small in-test JSON-array file qualifies for transcoding, and the test
 * inspects the cache directory afterwards to confirm an NDJSON file was materialized (proxy for
 * "the {@code read_ndjson_objects} path was taken").
 */
class JsonPathSourceHandlerTranscodeIntegrationTest {

    @TempDir
    Path tempDir;

    private Path cacheDir;

    private JsonNdjsonTranscodeCache cache;

    private Connection connection;

    @BeforeEach
    void beforeEach() throws IOException, SQLException {
        cacheDir = Files.createDirectory(tempDir.resolve("cache"));
        cache = new JsonNdjsonTranscodeCache(cacheDir, 256L);
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void afterEach() throws SQLException {
        cache.close();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void evaluate_largeJsonArray_routesThroughTranscodeCache() throws IOException {
        var sourceFile = tempDir.resolve("people.json");
        var recordCount = writeJsonArrayFile(sourceFile, 100);

        var view = createJsonView(sourceFile.toString(), "$[*]", expressionField("name", "name"));

        var evaluator = new DuckDbLogicalViewEvaluator(
                connection,
                false,
                /* sourceTableCache */ null,
                /* databaseAttacher */ null,
                cache,
                new Semaphore(1),
                Schedulers.boundedElastic());

        StepVerifier.create(evaluator
                        .evaluate(view, source -> null, EvaluationContext.defaults())
                        .collectList())
                .assertNext(iterations -> {
                    assertThat(iterations, hasSize(recordCount));
                    assertThat(iterations.get(0).getValue("name"), is(Optional.of("name_0")));
                    assertThat(iterations.get(recordCount - 1).getValue("name"), is(Optional.of("name_99")));
                })
                .verifyComplete();

        assertThat(cache.size(), is(greaterThanOrEqualTo(1)));
        try (Stream<Path> entries = Files.list(cacheDir)) {
            var ndjsonFiles = entries.filter(p -> p.getFileName().toString().endsWith(".ndjson"))
                    .toList();
            assertThat(ndjsonFiles, hasSize(1));
        }
    }

    @Test
    void evaluate_smallJsonArray_doesNotTriggerTranscode() throws IOException {
        var sourceFile = tempDir.resolve("tiny.json");
        Files.writeString(sourceFile, "[{\"name\":\"alice\"},{\"name\":\"bob\"}]", StandardCharsets.UTF_8);

        var view = createJsonView(sourceFile.toString(), "$[*]", expressionField("name", "name"));

        var evaluator = new DuckDbLogicalViewEvaluator(
                connection,
                false,
                /* sourceTableCache */ null,
                /* databaseAttacher */ null,
                cache,
                new Semaphore(1),
                Schedulers.boundedElastic());

        StepVerifier.create(evaluator
                        .evaluate(view, source -> null, EvaluationContext.defaults())
                        .collectList())
                .assertNext(iterations -> assertThat(iterations, hasSize(2)))
                .verifyComplete();

        try (Stream<Path> entries = Files.list(cacheDir)) {
            assertThat(entries.toList(), is(empty()));
        }
    }

    // --- helpers ---

    private static int writeJsonArrayFile(Path path, int recordCount) throws IOException {
        var padding = "x".repeat(50);
        try (OutputStream out = Files.newOutputStream(path)) {
            out.write('[');
            for (var i = 0; i < recordCount; i++) {
                if (i > 0) {
                    out.write(',');
                }
                var record = "{\"id\":%d,\"name\":\"name_%d\",\"pad\":\"%s\"}".formatted(i, i, padding);
                out.write(record.getBytes(StandardCharsets.UTF_8));
            }
            out.write(']');
        }
        return recordCount;
    }

    private static ExpressionField expressionField(String fieldName, String reference) {
        var field = mock(ExpressionField.class);
        lenient().when(field.getFieldName()).thenReturn(fieldName);
        lenient().when(field.getReference()).thenReturn(reference);
        return field;
    }

    private static LogicalView createJsonView(String filePath, String iterator, ExpressionField... fields) {
        var fileSource = mock(FileSource.class);
        lenient().when(fileSource.getUrl()).thenReturn(filePath);

        var refFormulation = mock(ReferenceFormulation.class);
        lenient().when(refFormulation.getAsResource()).thenReturn(Rdf.Ql.JsonPath);

        var logicalSource = mock(LogicalSource.class);
        lenient().when(logicalSource.getReferenceFormulation()).thenReturn(refFormulation);
        lenient().when(logicalSource.getSource()).thenReturn(fileSource);
        lenient().when(logicalSource.getIterator()).thenReturn(iterator);
        lenient()
                .when(logicalSource.resolveIteratorAsString())
                .thenAnswer(invocation -> Optional.ofNullable(iterator).filter(s -> !s.isBlank()));

        var view = mock(LogicalView.class);
        lenient().when(view.getViewOn()).thenReturn(logicalSource);
        lenient().when(view.getFields()).thenReturn(Set.of(fields));
        lenient().when(view.getResourceName()).thenReturn("testView");
        lenient().when(view.getStructuralAnnotations()).thenReturn(Set.of());

        return view;
    }
}
