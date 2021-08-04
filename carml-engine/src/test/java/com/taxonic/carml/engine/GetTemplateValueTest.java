package com.taxonic.carml.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.when;

import com.taxonic.carml.engine.template.Template;
import com.taxonic.carml.engine.template.TemplateParser;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GetTemplateValueTest {

  @Mock
  UnaryOperator<String> transformValue;

  @Mock
  Function<Object, String> createNaturalRdfLexicalForm;

  @Mock
  ExpressionEvaluation expressionEvaluation;

  @Test
  void getTemplateValue_givenValidInputAndFindingValue_performsAsExpected() {
    when(expressionEvaluation.apply("xyz")).thenReturn(Optional.of("evaluated"));
    when(createNaturalRdfLexicalForm.apply("evaluated")).thenReturn("natural");
    when(transformValue.apply("natural")).thenReturn("transformed");

    Template template = TemplateParser.build()
        .parse("abc{xyz}");
    GetTemplateValue getTemplateValue =
        new GetTemplateValue(template, template.getExpressions(), transformValue, createNaturalRdfLexicalForm);
    Optional<Object> templateValue = getTemplateValue.apply(expressionEvaluation);
    String result = unpackTemplateValue(templateValue);
    assertThat(result, is("abctransformed"));
  }

  private String unpackTemplateValue(Optional<Object> templateValue) {
    return templateValue.map(v -> {
      if (v instanceof List<?>) {
        List<?> list = (List<?>) v;
        assertThat(list.size(), is(1));
        return (String) (list).get(0);
      } else {
        throw new RuntimeException();
      }
    })
        .orElseThrow(RuntimeException::new);
  }

  @Test
  void getTemplateValue_givenValidInputWithMultipleExpressions_performsAsExpected() {
    when(expressionEvaluation.apply("xyz")).thenReturn(Optional.of("evaluated"));
    when(createNaturalRdfLexicalForm.apply("evaluated")).thenReturn("natural");
    when(transformValue.apply("natural")).thenReturn("transformed");

    Template template = TemplateParser.build()
        .parse("abc{xyz}{xyz}");
    GetTemplateValue getTemplateValue =
        new GetTemplateValue(template, template.getExpressions(), transformValue, createNaturalRdfLexicalForm);
    Optional<Object> templateValue = getTemplateValue.apply(expressionEvaluation);
    String result = unpackTemplateValue(templateValue);
    assertThat(result, is("abctransformedtransformed"));
  }

  @Test
  void getTemplateValue_givenValidInputAndNotFindingValue_returnsNoValues() {
    when(expressionEvaluation.apply("xyz")).thenReturn(Optional.empty());

    Template template = TemplateParser.build()
        .parse("abc{xyz}");
    GetTemplateValue getTemplateValue =
        new GetTemplateValue(template, template.getExpressions(), transformValue, createNaturalRdfLexicalForm);
    Optional<Object> templateValue = getTemplateValue.apply(expressionEvaluation);
    assertThat(templateValue, is(Optional.empty()));
  }

}
