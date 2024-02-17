package io.carml.engine.template;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.engine.template.CarmlTemplate.ExpressionSegment;
import io.carml.engine.template.CarmlTemplate.Text;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TemplateParserTest {

  private TemplateParser parser;

  @BeforeEach
  void createParser() {
    parser = TemplateParser.build();
  }

  @Test
  void testTrailingVariable() {
    testTemplate("abc{xyz}", new Text("abc"), new ExpressionSegment(0, "xyz"));
  }

  @Test
  void testTrailingText() {
    testTemplate("abc{xyz}x", new Text("abc"), new ExpressionSegment(0, "xyz"), new Text("x"));
  }

  @Test
  void testLeadingVariable() {
    testTemplate("{xyz}x", new ExpressionSegment(0, "xyz"), new Text("x"));
  }

  @Test
  void testVariableOnly() {
    testTemplate("{xyz}", new ExpressionSegment(0, "xyz"));
  }

  @Test
  void testTextOnly() {
    testTemplate("xyz", new Text("xyz"));
  }

  @Test
  void testEscaping() {
    testTemplate("xyz\\{", new Text("xyz{"));
  }

  @Test
  void testClosingBraceInText() {
    testTemplate("xyz}", new Text("xyz}"));
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
    Template expected = CarmlTemplate.build(Arrays.asList(expectedSegments));
    assertThat(expected, is(template));
  }
}
