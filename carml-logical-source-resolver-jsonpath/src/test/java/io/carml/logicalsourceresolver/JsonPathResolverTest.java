package io.carml.logicalsourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.carml.logicalsourceresolver.LogicalSourceResolver.LogicalSourceResolverFactory;
import io.carml.model.LogicalSource;
import io.carml.model.impl.CarmlLogicalSource;
import io.carml.model.impl.CarmlRelativePathSource;
import io.carml.util.TypeRef;
import io.carml.vocab.Rdf;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class JsonPathResolverTest {

    InputStream inputStream;

    private LogicalSourceResolverFactory<JsonNode> jsonPathResolverFactory;

    @BeforeEach
    public void init() {
        jsonPathResolverFactory = JsonPathResolver.factory();
    }

    @Test
    void givenRecordResolver_whenSuppliedResolvedSourceUnsupported_thenThrowException() {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .id("food")
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*]")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var countrySource = CarmlLogicalSource.builder()
                .id("country")
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*].countryOfOrigin")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var resolvedSource = ResolvedSource.of("food.json", new TypeRef<>() {});
        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());
        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource, countrySource));

        // When
        var exception = assertThrows(LogicalSourceResolverException.class, () -> recordResolver.apply(resolvedSource));

        // Then
        assertThat(exception.getMessage(), startsWith("Unsupported source object provided for logical sources:"));
    }

    @Test
    void givenRecordResolverAndLogicalSources_whenGetRecordResolver_thenReturnSourceFluxWithMatchingObjects() {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*]")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var countrySource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*].countryOfOrigin")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");
        var resolvedSource = ResolvedSource.of(inputStream, new TypeRef<>() {});
        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());

        // When
        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource, countrySource));
        Flux<Object> records = recordResolver
                .apply(resolvedSource)
                .flatMap(logicalSourceRecord -> stringFlux(logicalSourceRecord.getSourceRecord()));

        // Then
        StepVerifier.create(records).expectNextCount(18).verifyComplete();
    }

    @Test
    void givenRecordResolverAndLogicalSources_whenGetRecordResolverTwice_thenReturnSourceFluxWithMatchingObjects() {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*]")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var countrySource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*].countryOfOrigin")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");
        var resolvedSource = ResolvedSource.of(inputStream, new TypeRef<>() {});
        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());

        // When
        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource, countrySource));
        Flux<Object> records = recordResolver
                .apply(resolvedSource)
                .flatMap(logicalSourceRecord -> stringFlux(logicalSourceRecord.getSourceRecord()));

        // Then
        StepVerifier.create(records).expectNextCount(18).verifyComplete();

        // Given
        var foodSource2 = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*]")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var countrySource2 = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*].countryOfOrigin")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");
        var resolvedSource2 = ResolvedSource.of(inputStream, new TypeRef<>() {});

        // When
        var recordResolver2 = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource2, countrySource2));
        Flux<Object> records2 = recordResolver2
                .apply(resolvedSource2)
                .flatMap(logicalSourceRecord -> stringFlux(logicalSourceRecord.getSourceRecord()));

        // Then
        StepVerifier.create(records2).expectNextCount(18).verifyComplete();
    }

    private Flux<String> stringFlux(Object object) {
        return Flux.merge(Flux.just("a" + object), Flux.just("b" + object), Flux.just("c" + object));
    }

    @Test
    void givenCompletedLogicalSourceRecordResolver_whenRequest_thenComplete() {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*]")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var countrySource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*].countryOfOrigin")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");
        var resolvedSource = ResolvedSource.of(inputStream, new TypeRef<>() {});

        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());
        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource, countrySource));
        var items = recordResolver.apply(resolvedSource);

        // When
        var allRecordsGenerated = StepVerifier.create(items, 6).expectNextCount(6);

        // Then
        allRecordsGenerated.thenRequest(1).expectNextCount(0).verifyComplete();
    }

    @Test
    void givenJsonPathResolverWithSmallBufferSize_whenGetRecordResolver_thenReturnSourceFluxWithMatchingObjects() {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*]")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var countrySource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*].countryOfOrigin")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");
        var resolvedSource = ResolvedSource.of(inputStream, new TypeRef<>() {});

        var factory = JsonPathResolver.factory(200);
        var jsonPathResolver = factory.apply(foodSource.getSource());
        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource, countrySource));

        // When
        var items = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(items, 1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void givenJsonPathResolverAndProvidedRecord_whenGetRecordResolver_thenReturnSourceFluxWithMatchingObjects()
            throws IOException {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*]")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var countrySource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.food[*].countryOfOrigin")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var record = new ObjectMapper().readTree(JsonPathResolverTest.class.getResourceAsStream("food.json"));
        var resolvedSource = ResolvedSource.of(record, new TypeRef<>() {});

        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());

        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource, countrySource));

        // When
        var items = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(items, 1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(100))
                .thenRequest(1)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void givenRecordResolverWithProvidedRecord_whenGetRecordResolver_thenReturnSourceFluxWithRecord() {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        Map<String, Object> waffles = Map.of("name", "Belgian Waffles", "countryOfOrigin", "Belgium");

        var record = new ObjectMapper().valueToTree(waffles);
        var resolvedSource = ResolvedSource.of(record, new TypeRef<>() {});

        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());

        // When
        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource));
        var records = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(records)
                .expectNextMatches(logicalSourceRecord ->
                        logicalSourceRecord.getSourceRecord().equals(record))
                .verifyComplete();
    }

    @Test
    void givenProvidedRecordAndSingleValueIterator_whenGetRecordResolver_thenReturnSourceFluxWithRecord() {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.name")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        Map<String, Object> waffles = Map.of("name", "Belgian Waffles", "countryOfOrigin", "Belgium");

        var record = new ObjectMapper().valueToTree(waffles);
        var resolvedSource = ResolvedSource.of(record, new TypeRef<>() {});

        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());

        // When
        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource));
        var records = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(records)
                .expectNextMatches(logicalSourceRecord ->
                        logicalSourceRecord.getSourceRecord().equals(new ObjectMapper().valueToTree("Belgian Waffles")))
                .verifyComplete();
    }

    @Test
    void givenProvidedRecordAndNonResolvingIterator_whenGetRecordResolver_thenReturnSourceFluxWithRecord() {
        // Given
        var nonResolvingSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.foo")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        Map<String, Object> waffles = Map.of("name", "Belgian Waffles", "countryOfOrigin", "Belgium");

        var record = new ObjectMapper().valueToTree(waffles);
        var resolvedSource = ResolvedSource.of(record, new TypeRef<>() {});

        var jsonPathResolver = jsonPathResolverFactory.apply(nonResolvingSource.getSource());

        // When
        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(nonResolvingSource));
        var records = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(records).expectNextCount(0).verifyComplete();
    }

    @Test
    void givenInputAndJsonPathExpression_whenEvaluateExpressionApply_executesJsonPathCorrectly() throws IOException {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.foo")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var food = IOUtils.toString(
                Objects.requireNonNull(JsonPathResolverTest.class.getResourceAsStream("food.json")),
                StandardCharsets.UTF_8);
        var objectMapper = new ObjectMapper();

        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());

        var expressionEvaluationFactory = jsonPathResolver.getExpressionEvaluationFactory();
        var expressionEvaluation = expressionEvaluationFactory.apply(objectMapper.readTree(food));

        // When
        var evaluationResult = expressionEvaluation.apply("$.food[*].name");

        // Then
        var results =
                evaluationResult.map(ExpressionEvaluation::extractStringValues).orElse(List.of());

        assertThat(results, hasSize(3));
        assertThat(results, hasItems("Belgian Waffles", "French Toast", "Dutch Pancakes"));
    }

    @Test
    void givenUnresolvableJsonPath_whenSourceFluxApplied_shouldReturnEmptyFlux() {
        // Given
        var foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.foo")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());

        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource));
        inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");
        var resolvedSource = ResolvedSource.of(inputStream, new TypeRef<>() {});

        // When
        var items = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(items).verifyComplete();
    }

    @Test
    void givenInvalidJsonPath_whenSourceFluxApplied_shouldThrowException() {
        // Given
        LogicalSource foodSource = CarmlLogicalSource.builder()
                .source(CarmlRelativePathSource.of("food.json"))
                .iterator("$.foo[invalid]")
                .referenceFormulation(Rdf.Ql.JsonPath)
                .build();

        var jsonPathResolver = jsonPathResolverFactory.apply(foodSource.getSource());

        var recordResolver = jsonPathResolver.getLogicalSourceRecords(Set.of(foodSource));
        inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");
        var resolvedSource = ResolvedSource.of(inputStream, new TypeRef<>() {});

        // When
        var items = recordResolver.apply(resolvedSource);

        // Then
        StepVerifier.create(items)
                .expectErrorMatches(throwable ->
                        throwable.getMessage().equals("An exception occurred while parsing expression: $.foo[invalid]"))
                .verify();
    }
}
