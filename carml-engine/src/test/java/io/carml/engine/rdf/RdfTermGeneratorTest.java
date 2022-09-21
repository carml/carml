package io.carml.engine.rdf;


import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.carml.engine.ExpressionEvaluation;
import io.carml.engine.template.TemplateParser;
import io.carml.model.impl.CarmlObjectMap;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Optional;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RdfTermGeneratorTest {

  @Mock
  ExpressionEvaluation expressionEvaluation;

  private RdfTermGeneratorFactory rdfTermGeneratorFactory;

  @BeforeEach
  void beforeEach() {
    var rdfTermGeneratorConfig = RdfTermGeneratorConfig.builder()
        .baseIri(iri("http://example.com/base/"))
        .valueFactory(SimpleValueFactory.getInstance())
        .normalizationForm(Normalizer.Form.NFC)
        .build();
    rdfTermGeneratorFactory = RdfTermGeneratorFactory.of(rdfTermGeneratorConfig, TemplateParser.build());
  }

  @Test
  void givenRdfTermGenerator_whenExpressionEvaluationReturnsEmpty_thenResultEmpty() {
    // Given
    var objectMap = CarmlObjectMap.builder()
        .id("obj-map-1")
        .template("http://{foo}")
        .build();

    var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);
    when(expressionEvaluation.apply(any())).thenReturn(Optional.empty());

    // When
    var objects = objectGenerator.apply(expressionEvaluation);

    // Then
    assertThat(objects, is(empty()));
  }

  @Test
  void givenRdfTermGenerator_whenExpressionEvaluationReturnsListWithOnlyNull_thenResultEmpty() {
    // Given
    var objectMap = CarmlObjectMap.builder()
        .id("obj-map-1")
        .reference("foo")
        .build();

    var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);
    var nullList = new ArrayList<>();
    nullList.add(null);
    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(nullList));

    // When
    var objects = objectGenerator.apply(expressionEvaluation);

    // Then
    assertThat(objects, is(empty()));
  }

  @Test
  void givenRdfTermGenerator_whenExpressionEvaluationReturnsListWithNullAndNonNull_thenResultWithoutNulls() {
    // Given
    var nullList = new ArrayList<>();
    nullList.add(null);
    nullList.add("bar");
    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(nullList));

    var objectMap = CarmlObjectMap.builder()
        .id("obj-map-1")
        .reference("foo")
        .build();
    var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

    // When
    var objects = objectGenerator.apply(expressionEvaluation);

    // Then
    assertThat(objects, hasItems(SimpleValueFactory.getInstance()
        .createLiteral("bar")));
  }
}
