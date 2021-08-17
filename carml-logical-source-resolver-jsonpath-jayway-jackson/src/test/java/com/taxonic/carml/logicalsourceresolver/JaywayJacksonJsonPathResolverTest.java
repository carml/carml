package com.taxonic.carml.logicalsourceresolver;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.InvalidPathException;
import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class JaywayJacksonJsonPathResolverTest {

  InputStream inputStream;

  private JaywayJacksonJsonPathResolver jsonPathResolver;

  private ObjectMapper objectMapper;

  @BeforeEach
  public void init() {
    jsonPathResolver = JaywayJacksonJsonPathResolver.getInstance();
    objectMapper = new ObjectMapper();
  }

  @Test
  void givenJsonPathExpressionAndJsonInputStreamInput_whenApplySourceFlux_thenReturnSourceFluxWithMatchingObjects()
      throws IOException {
    // Given
    LogicalSource foodSource = new CarmlLogicalSource("", "$.food[*]", Rdf.Ql.JsonPath);
    LogicalSourceResolver.SourceFlux<Object> sourceFlux = jsonPathResolver.getSourceFlux();

    inputStream = JaywayJacksonJsonPathResolverTest.class.getResourceAsStream("food.json");

    // When
    Flux<Object> items = sourceFlux.apply(inputStream, foodSource);

    // Then
    StepVerifier.create(items)
        .expectNextCount(3)
        .verifyComplete();
  }

  @Test
  void givenJsonPathExpressionAndJsonObjectInput_whenApplySourceFlux_thenReturnSourceFluxWithMatchingObjects()
      throws IOException {
    // Given
    LogicalSource foodSource = new CarmlLogicalSource("", "$.food[*]", Rdf.Ql.JsonPath);
    LogicalSourceResolver.SourceFlux<Object> sourceFlux = jsonPathResolver.getSourceFlux();

    inputStream = JaywayJacksonJsonPathResolverTest.class.getResourceAsStream("food.json");
    Map<String, Object> json = objectMapper.readValue(inputStream, new TypeReference<>() {});

    // When
    Flux<Object> items = sourceFlux.apply(json, foodSource);

    // Then
    StepVerifier.create(items)
        .expectNextCount(3)
        .verifyComplete();
  }

  @Test
  void givenInputAndJsonPathExpression_whenEvaluateExpressionApply_executesJsonPathCorrectly() throws IOException {
    // Given
    String food = IOUtils.toString(
        Objects.requireNonNull(JaywayJacksonJsonPathResolverTest.class.getResourceAsStream("food.json")),
        StandardCharsets.UTF_8);

    Map<String, Object> json = objectMapper.readValue(food, new TypeReference<>() {});

    LogicalSourceResolver.ExpressionEvaluationFactory<Object> expressionEvaluationFactory =
        jsonPathResolver.getExpressionEvaluationFactory();
    ExpressionEvaluation expressionEvaluation = expressionEvaluationFactory.apply(json);

    // When
    Optional<Object> evaluationResult = expressionEvaluation.apply("$.food[*].name");

    // Then
    List<String> results = evaluationResult.map(ExpressionEvaluation::extractValues)
        .orElse(List.of());

    assertThat(results, hasSize(3));
    assertThat(results, hasItems("Belgian Waffles", "French Toast", "Dutch Pancakes"));
  }

  @Test
  void givenUnresolvableJsonPath_whenSourceFluxApplied_shouldReturnEmptyFlux() throws IOException {
    // Given
    LogicalSource foodSource = new CarmlLogicalSource("", "foo", Rdf.Ql.JsonPath);
    LogicalSourceResolver.SourceFlux<Object> sourceFlux = jsonPathResolver.getSourceFlux();

    inputStream = JaywayJacksonJsonPathResolverTest.class.getResourceAsStream("food.json");
    Map<String, Object> json = objectMapper.readValue(inputStream, new TypeReference<>() {});

    // When
    Flux<Object> items = sourceFlux.apply(json, foodSource);

    // Then
    StepVerifier.create(items)
        .expectComplete();
  }

  @Test
  void givenInvalidJsonPath_whenSourceFluxApplied_shouldThrowException() throws IOException {
    // Given
    LogicalSource foodSource = new CarmlLogicalSource("", "foo[invalid]", Rdf.Ql.JsonPath);
    LogicalSourceResolver.SourceFlux<Object> sourceFlux = jsonPathResolver.getSourceFlux();

    inputStream = JaywayJacksonJsonPathResolverTest.class.getResourceAsStream("food.json");
    Map<String, Object> json = objectMapper.readValue(inputStream, new TypeReference<>() {});

    // When
    Throwable exception = assertThrows(InvalidPathException.class, () -> sourceFlux.apply(json, foodSource));

    // Then
    assertThat(exception.getMessage(), startsWith("Could not parse"));
  }
}
