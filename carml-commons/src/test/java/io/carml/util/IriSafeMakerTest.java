package io.carml.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.text.Normalizer.Form;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class IriSafeMakerTest {

  private Function<String, String> safeMaker = IriSafeMaker.create(Form.NFC, true);

  private Function<String, String> lcSafeMaker = IriSafeMaker.create(Form.NFC, false);

  private Function<String, String> nfkcSafeMaker = IriSafeMaker.create(Form.NFKC, true);

  @Test
  void safeMaker_givenStringWithSpace_encodesAsExpected() {
    test("hello there", "hello%20there", true);
  }

  @Test
  void safeMaker_givenNotToBeEncodedString_DoesNothing() {
    test("test-tester", "test-tester", true);
    test("test_tester", "test_tester", true);
    test("test.tester", "test.tester", true);
    test("~test", "~test", true);
    test("葉篤正", "葉篤正", true);
    test("StandaardGeluidsruimteDagInDb_a_M²", "StandaardGeluidsruimteDagInDb_a_M²", true);
  }

  @Test
  void safeMaker_givenParentheses_encodesAsExpected() {
    test("(test)", "%28test%29", true);
  }

  @Test
  void safeMaker_givenUri_encodesAsExpected() {
    test("http://example.com", "http%3A%2F%2Fexample.com", true);
  }

  @Test
  void safeMaker_givenEncodingToken_encodesAsExpected() {
    test("100%", "100%25", true);
    test("1,2", "1%2c2", false);
  }

  @Test
  void nfkcSafeMaker_givenNormalizableToken_encodesAsExpected() {
    String input = "StandaardGeluidsruimteDagInDb_a_M²";
    String expected = "StandaardGeluidsruimteDagInDb_a_M2";
    String actual = nfkcSafeMaker.apply(input);
    assertThat(expected, is(actual));
  }

  private void test(String toMakeIriSafe, String expectedResult, boolean upperCasePercentEncoding) {
    String iriSafeValue = makeSafe(toMakeIriSafe, upperCasePercentEncoding);
    assertThat(expectedResult, is(iriSafeValue));
  }

  private String makeSafe(String input, boolean upperCasePercentEncoding) {
    return upperCasePercentEncoding ? safeMaker.apply(input) : lcSafeMaker.apply(input);
  }

}
