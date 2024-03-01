package io.carml.engine.rdf;


import static org.eclipse.rdf4j.model.util.Values.iri;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.carml.logicalsourceresolver.DatatypeMapper;
import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.model.impl.CarmlDatatypeMap;
import io.carml.model.impl.CarmlLanguageMap;
import io.carml.model.impl.CarmlObjectMap;
import io.carml.model.impl.template.TemplateParser;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
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

  @Mock
  DatatypeMapper datatypeMapper;

  private RdfTermGeneratorFactory rdfTermGeneratorFactory;

  @BeforeEach
  void beforeEach() {
    var rdfTermGeneratorConfig = RdfTermGeneratorConfig.builder()
        .baseIri(iri("http://example.com/base/"))
        .valueFactory(SimpleValueFactory.getInstance())
        .normalizationForm(Normalizer.Form.NFC)
        .build();
    rdfTermGeneratorFactory = RdfTermGeneratorFactory.of(rdfTermGeneratorConfig, TemplateParser.getInstance());
  }

  @Test
  void givenRdfTermGenerator_whenExpressionEvaluationReturnsEmpty_thenResultEmpty() {
    // Given
    var objectMap = CarmlObjectMap.builder()
        .id("obj-map-1")
        .template(TemplateParser.getInstance()
            .parse("http://{foo}"))
        .build();

    var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);
    when(expressionEvaluation.apply(any())).thenReturn(Optional.empty());
    when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

    // When
    var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

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

    var nullList = new ArrayList<>();
    nullList.add(null);
    var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(nullList));
    when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

    // When
    var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

    // Then
    assertThat(objects, is(empty()));
  }

  @Test
  void givenRdfTermGenerator_whenExpressionEvaluationReturnsListWithNullAndNonNull_thenResultWithoutNulls() {
    // Given
    var nullList = new ArrayList<>();
    nullList.add(null);
    nullList.add("bar");

    var objectMap = CarmlObjectMap.builder()
        .id("obj-map-1")
        .reference("foo")
        .build();
    var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(nullList));
    when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

    // When
    var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

    // Then
    assertThat(objects, hasItems(SimpleValueFactory.getInstance()
        .createLiteral("bar")));
  }

  @Test
  void givenObjectMapWithLanguageMap_whenObjectGeneratorApplied_thenReturnLiteralWithLang() {
    // Given
    var objectMap = CarmlObjectMap.builder()
        .id("obj-map-1")
        .reference("foo")
        .languageMap(CarmlLanguageMap.builder()
            .reference("foo")
            .build())
        .build();

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("bar")));
    when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

    var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

    // When
    var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

    // Then
    assertThat(objects, hasItems(SimpleValueFactory.getInstance()
        .createLiteral("bar", "bar")));
  }

  @Test
  void givenObjectMapWithDatatypeMap_whenObjectGeneratorApplied_thenReturnLiteralWithDatatype() {
    // Given
    var objectMap = CarmlObjectMap.builder()
        .id("obj-map-1")
        .reference("foo")
        .datatypeMap(CarmlDatatypeMap.builder()
            .template(TemplateParser.getInstance()
                .parse("https://{foo}.com"))
            .build())
        .build();

    when(expressionEvaluation.apply(any())).thenReturn(Optional.of(List.of("bar")));
    when(datatypeMapper.apply(any())).thenReturn(Optional.empty());

    var objectGenerator = rdfTermGeneratorFactory.getObjectGenerator(objectMap);

    // When
    var objects = objectGenerator.apply(expressionEvaluation, datatypeMapper);

    // Then
    assertThat(objects, hasItems(SimpleValueFactory.getInstance()
        .createLiteral("bar", iri("https://bar.com"))));
  }
}
