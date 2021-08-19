package com.taxonic.carml.logicalsourceresolver;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class JsonPathResolverTest {

  InputStream inputStream;

  private JsonPathResolver jsonPathResolver;

  @BeforeEach
  public void init() {
    jsonPathResolver = JsonPathResolver.getInstance();
  }

  @Test
  void givenJsonPathExpression_whenGetSourceFlux_givenJsonPath_thenReturnSourceFluxWithMatchingObjects() {
    // Given
    LogicalSource foodSource = CarmlLogicalSource.builder()
        .source("")
        .iterator("$.food[*]")
        .referenceFormulation(Rdf.Ql.JsonPath)
        .build();

    inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");

    LogicalSourceResolver.SourceFlux<Object> sourceFlux = jsonPathResolver.getSourceFlux();

    // When
    Flux<Object> items = sourceFlux.apply(inputStream, foodSource);

    // Then
    StepVerifier.create(items)
        .expectNextCount(3)
        .verifyComplete();
  }

  @Test
  void givenInputAndJsonPathExpression_whenEvaluateExpressionApply_executesJsonPathCorrectly() throws IOException {
    // Given
    String food = IOUtils.toString(Objects.requireNonNull(JsonPathResolverTest.class.getResourceAsStream("food.json")),
        StandardCharsets.UTF_8);

    LogicalSourceResolver.ExpressionEvaluationFactory<Object> expressionEvaluationFactory =
        jsonPathResolver.getExpressionEvaluationFactory();
    ExpressionEvaluation expressionEvaluation = expressionEvaluationFactory.apply(food);

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
    LogicalSource foodSource = CarmlLogicalSource.builder()
        .source("")
        .iterator("foo")
        .referenceFormulation(Rdf.Ql.JsonPath)
        .build();
    LogicalSourceResolver.SourceFlux<Object> sourceFlux = jsonPathResolver.getSourceFlux();

    inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");

    // When
    Flux<Object> items = sourceFlux.apply(inputStream, foodSource);

    // Then
    StepVerifier.create(items)
        .expectComplete();
  }

  @Disabled("Why doesn't json surfer throw exception?")
  @Test
  void givenInvalidJsonPath_whenSourceFluxApplied_shouldThrowException() throws IOException {
    // Given
    LogicalSource foodSource = CarmlLogicalSource.builder()
        .source("")
        .iterator("foo[invalid]")
        .referenceFormulation(Rdf.Ql.JsonPath)
        .build();
    LogicalSourceResolver.SourceFlux<Object> sourceFlux = jsonPathResolver.getSourceFlux();

    inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");

    // When
    Throwable exception = assertThrows(RuntimeException.class, () -> sourceFlux.apply(inputStream, foodSource));

    // Then
    // TODO
  }

}
