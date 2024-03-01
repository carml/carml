package io.carml.model.impl.template;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.model.Template;
import io.carml.model.impl.CarmlTemplate;
import io.carml.model.impl.CarmlTemplate.ExpressionSegment;
import io.carml.model.impl.CarmlTemplate.TextSegment;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TemplateParserTest {

  private TemplateParser parser;

  @BeforeEach
  void createParser() {
    parser = TemplateParser.getInstance();
  }

  @Test
  void testTrailingVariable() {
    testTemplate("abc{xyz}", new TextSegment("abc"), new ExpressionSegment(0, "xyz"));
  }

  @Test
  void testTrailingTextSegment() {
    testTemplate("abc{xyz}x", new TextSegment("abc"), new ExpressionSegment(0, "xyz"), new TextSegment("x"));
  }

  @Test
  void testLeadingVariable() {
    testTemplate("{xyz}x", new ExpressionSegment(0, "xyz"), new TextSegment("x"));
  }

  @Test
  void testVariableOnly() {
    testTemplate("{xyz}", new ExpressionSegment(0, "xyz"));
  }

  @Test
  void testTextOnly() {
    testTemplate("xyz", new TextSegment("xyz"));
  }

  @Test
  void testEscaping() {
    testTemplate("xyz\\{", new TextSegment("xyz{"));
  }

  @Test
  void testClosingBraceInTextSegment() {
    testTemplate("xyz}", new TextSegment("xyz}"));
  }

  @Test
  void testMultipleVariables() {
    testTemplate("{abc}{xyz}", new ExpressionSegment(0, "abc"), new ExpressionSegment(1, "xyz"));
  }

  @Test
  void givenTemplateWithUnclosedExpression_whenParse_thenThrowsException() {
    // Given
    var templateString = "{abc}{xyz";

    // When
    var templateException = assertThrows(TemplateException.class, () -> parser.parse(templateString));

    // Then
    assertThat(templateException.getMessage(), is("unclosed expression in template [{abc}{xyz]"));
  }

  @Test
  void givenTemplateNestedCurlyBracket_whenParse_thenThrowsException() {
    // Given
    var templateString = "{abc{xyz}}";

    // When
    var templateException = assertThrows(TemplateException.class, () -> parser.parse(templateString));

    // Then
    assertThat(templateException.getMessage(), is("encountered unescaped nested { character in template [{abc{xyz}}]"));
  }

  @Test
  void givenTemplateWithInvalidEscapeCharacters_whenParse_thenThrowsException() {
    // Given
    var templateString = "{abc\\xyz}";

    // When
    var templateException = assertThrows(TemplateException.class, () -> parser.parse(templateString));

    // Then
    assertThat(templateException.getMessage(),
        is("invalid escape sequence in template [{abc\\xyz}] - escaping char [x]"));
  }

  @Test
  void testEmpty() {
    testTemplate("");
  }

  private void testTemplate(String templateStr, CarmlTemplate.Segment... expectedSegments) {
    Template template = parser.parse(templateStr);
    Template expected = CarmlTemplate.of(Arrays.asList(expectedSegments));
    assertThat(expected, is(template));
  }
}
