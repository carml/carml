package com.taxonic.carml.engine.template;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.taxonic.carml.engine.template.CarmlTemplate.ExpressionSegment;
import com.taxonic.carml.engine.template.CarmlTemplate.Text;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TemplateParserTest {

  private TemplateParser parser;

  @BeforeEach
  public void createParser() {
    parser = TemplateParser.build();
  }

  @Test
  public void testTrailingVariable() {
    testTemplate("abc{xyz}", new Text("abc"), new ExpressionSegment(0, "xyz"));
  }

  @Test
  public void testTrailingText() {
    testTemplate("abc{xyz}x", new Text("abc"), new ExpressionSegment(0, "xyz"), new Text("x"));
  }

  @Test
  public void testLeadingVariable() {
    testTemplate("{xyz}x", new ExpressionSegment(0, "xyz"), new Text("x"));
  }

  @Test
  public void testVariableOnly() {
    testTemplate("{xyz}", new ExpressionSegment(0, "xyz"));
  }

  @Test
  public void testTextOnly() {
    testTemplate("xyz", new Text("xyz"));
  }

  @Test
  public void testEscaping() {
    testTemplate("xyz\\{", new Text("xyz{"));
  }

  @Test
  public void testClosingBraceInText() {
    testTemplate("xyz}", new Text("xyz}"));
  }

  @Test
  public void testMultipleVariables() {
    testTemplate("{abc}{xyz}", new ExpressionSegment(0, "abc"), new ExpressionSegment(1, "xyz"));
  }

  @Test
  public void testEmpty() {
    testTemplate("");
  }

  private void testTemplate(String templateStr, CarmlTemplate.Segment... expectedSegments) {
    Template template = parser.parse(templateStr);
    Template expected = CarmlTemplate.build(Arrays.asList(expectedSegments));
    assertThat(expected, is(template));
  }

}
