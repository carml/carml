package io.carml.engine.rdf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.NonExistentReferenceException;
import io.carml.util.RmlMappingLoader;
import io.carml.util.TypeRef;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.eclipse.rdf4j.model.util.ModelCollector;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class StrictModeTest {

    @Test
    void givenNonExistentReference_whenMapWithStrictMode_thenThrowNonExistentReferenceException() {
        // Given
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, resource("strict-mode-nonexistent-ref.rml.ttl"));
        var rmlMapper =
                RdfRmlMapper.builder().triplesMaps(mapping).strictMode(true).build();

        // When / Then — use mapRecord (LS path) so strict mode validation fires
        var resultMono = rmlMapper
                .mapRecord(Mono.just(resource("strict-mode-data.json")), new TypeRef<>() {})
                .collect(ModelCollector.toTreeModel());
        var exception = assertThrows(NonExistentReferenceException.class, resultMono::block);

        assertThat(exception.getMessage(), containsString("$.THIS_VALUE_DOES_NOT_EXIST"));
        assertThat(exception.getMessage(), containsString("never produced a non-null result"));
    }

    @Test
    void givenNonExistentReference_whenMapWithLenientMode_thenSucceedSilently() {
        // Given
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, resource("strict-mode-nonexistent-ref.rml.ttl"));
        var rmlMapper =
                RdfRmlMapper.builder().triplesMaps(mapping).strictMode(false).build();

        // When / Then
        var model = assertDoesNotThrow(() -> rmlMapper
                .map(resource("strict-mode-data.json"))
                .collect(ModelCollector.toTreeModel())
                .block());

        assertThat(model, is(notNullValue()));
    }

    @Test
    void givenDefaultMode_whenMapWithNonExistentReference_thenSucceedSilently() {
        // Given - default mode (no explicit strictMode call)
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, resource("strict-mode-nonexistent-ref.rml.ttl"));
        var rmlMapper = RdfRmlMapper.builder().triplesMaps(mapping).build();

        // When / Then - should not throw (lenient is default)
        assertDoesNotThrow(() -> rmlMapper
                .map(resource("strict-mode-data.json"))
                .collect(ModelCollector.toTreeModel())
                .block());
    }

    @Test
    void givenAllReferencesExist_whenMapWithStrictMode_thenSucceedWithoutError() {
        // Given
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, resource("cars.rml.ttl"));
        var rmlMapper =
                RdfRmlMapper.builder().triplesMaps(mapping).strictMode(true).build();

        // When / Then - all references exist, so strict mode should succeed
        var model = assertDoesNotThrow(() -> rmlMapper
                .map(resource("cars.csv"))
                .collect(ModelCollector.toTreeModel())
                .block());

        assertThat(model, is(notNullValue()));
        assertThat(model.size(), is(21));
    }

    @Test
    void givenEmptyLogicalSource_whenMapWithStrictMode_thenCompleteWithoutError() {
        // Given - empty source means no records to evaluate; strict mode should not false-positive
        var mapping = RmlMappingLoader.build().load(RDFFormat.TURTLE, resource("strict-mode-nonexistent-ref.rml.ttl"));
        var rmlMapper =
                RdfRmlMapper.builder().triplesMaps(mapping).strictMode(true).build();

        var emptySource = new ByteArrayInputStream("{\"students\":[]}".getBytes(StandardCharsets.UTF_8));

        // When / Then - no records processed, so validation should be skipped
        assertDoesNotThrow(() ->
                rmlMapper.map(emptySource).collect(ModelCollector.toTreeModel()).block());
    }

    private static InputStream resource(String name) {
        return Objects.requireNonNull(
                StrictModeTest.class.getResourceAsStream(name), () -> "Resource not found: " + name);
    }
}
