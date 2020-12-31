package com.taxonic.carml.logical_source_resolver;


import com.taxonic.carml.engine.ExpressionEvaluation;
import com.taxonic.carml.model.LogicalSource;
import com.taxonic.carml.model.impl.CarmlLogicalSource;
import com.taxonic.carml.vocab.Rdf;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    LogicalSource foodSource = new CarmlLogicalSource("", "$.food[*]", Rdf.Ql.JsonPath);

    // When
    LogicalSourceResolver.SourceFlux<Object> sourceFlux = jsonPathResolver.getSourceFlux();

    inputStream = JsonPathResolverTest.class.getResourceAsStream("food.json");

    // Then
    StepVerifier.create(sourceFlux.apply(inputStream, foodSource))
        .expectNextCount(3)
        .verifyComplete();
  }

  @Test
  void givenInputAndJsonPathExpression_whenEvaluateExpressionApply_executesJsonPathCorrectly() throws IOException {
    String food = IOUtils.toString(JsonPathResolverTest.class.getResourceAsStream("food.json"), StandardCharsets.UTF_8);

    LogicalSourceResolver.ExpressionEvaluationFactory<Object> expressionEvaluationFactory =
        jsonPathResolver.getExpressionEvaluationFactory();
    ExpressionEvaluation expressionEvaluation = expressionEvaluationFactory.apply(food);

    Optional<Object> bla = expressionEvaluation.apply("$.food[*].name");

    List<String> results = bla.map(ExpressionEvaluation::extractValues)
        .orElse(List.of());

    System.out.println(results);
  }

  // @Test
  // public void sourceIterator_givenUnresolvableJsonPath_shouldReturnEmptyIterable() {
  // LogicalSource unresolvable = new CarmlLogicalSource(SOURCE, "foo", Rdf.Ql.JsonPath);
  // Iterable<Object> objectIterable = jsonPathResolver.bindSource(unresolvable, sourceResolver)
  // .get();
  //
  // assertThat(Iterables.size(objectIterable), is(0));
  // }
  //
  // @Test
  // public void sourceIterator_givenInvalidJsonPath_shouldThrowException() {
  // LogicalSource unresolvable = new CarmlLogicalSource(SOURCE, "food[invalid]", Rdf.Ql.JsonPath);
  // InvalidPathException exception =
  // assertThrows(InvalidPathException.class, () -> jsonPathResolver.bindSource(unresolvable,
  // sourceResolver)
  // .get());
  //
  // assertThat(exception.getMessage(), startsWith("Could not parse"));
  // }
}
