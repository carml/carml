package io.carml.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.text.Normalizer.Form;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class IriSafeMakerTest {

    private final Function<String, String> safeMaker = IriSafeMaker.create(Form.NFC, true);

    private final Function<String, String> lcSafeMaker = IriSafeMaker.create(Form.NFC, false);

    private final Function<String, String> nfkcSafeMaker = IriSafeMaker.create(Form.NFKC, true);

    private final Function<String, String> uriSafeMaker = IriSafeMaker.createUriSafe(Form.NFC);

    @Test
    void safeMaker_givenStringWithSpace_encodesAsExpected() {
        test("hello there", "hello%20there", true);
    }

    @Test
    void safeMaker_givenStringWithNewline_encodesAsExpected() {
        test("hello\nthere", "hello%0Athere", true);
        test("hello\r\nthere", "hello%0D%0Athere", true);
    }

    @Test
    void safeMaker_givenNotToBeEncodedString_DoesNothing() {
        test("test-tester", "test-tester", true);
        test("test_tester", "test_tester", true);
        test("test.tester", "test.tester", true);
        test("~test", "~test", true);
        test("葉篤正", "葉篤正", true);
        test("StandaardGeluidsruimteDagInDb_a_M²", "StandaardGeluidsruimteDagInDb_a_M²", true);
        test("öɦ﹩4", "öɦ﹩4", true);
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

    @Test
    void uriSafeMaker_givenUnicodeCharacters_encodesAsUtf8Bytes() {
        assertThat(uriSafeMaker.apply("Zoë"), is("Zo%C3%AB"));
        assertThat(uriSafeMaker.apply("Krüger"), is("Kr%C3%BCger"));
    }

    @Test
    void uriSafeMaker_givenSpace_encodesAsPercent20() {
        assertThat(uriSafeMaker.apply("hello there"), is("hello%20there"));
    }

    @Test
    void uriSafeMaker_givenTilde_preservesTilde() {
        assertThat(uriSafeMaker.apply("~test"), is("~test"));
    }

    @Test
    void uriSafeMaker_givenPercentSign_doubleEncodes() {
        assertThat(uriSafeMaker.apply("100%"), is("100%25"));
    }

    @Test
    void uriSafeMaker_givenPlusSign_encodesPlus() {
        assertThat(uriSafeMaker.apply("a+b"), is("a%2Bb"));
    }

    @Test
    void uriSafeMaker_givenAsciiSafe_doesNotEncode() {
        assertThat(uriSafeMaker.apply("test-value"), is("test-value"));
        assertThat(uriSafeMaker.apply("test_value"), is("test_value"));
        assertThat(uriSafeMaker.apply("test.value"), is("test.value"));
    }

    @Test
    void iriSafe_vs_uriSafe_givenUnicode_encodeDifferently() {
        var input = "Zoë Krüger";

        // IRI-safe (RFC 3987): preserves Unicode, encodes space
        assertThat(safeMaker.apply(input), is("Zoë%20Krüger"));

        // URI-safe (RFC 3986): encodes Unicode as UTF-8 bytes, encodes space
        assertThat(uriSafeMaker.apply(input), is("Zo%C3%AB%20Kr%C3%BCger"));
    }

    private void test(String toMakeIriSafe, String expectedResult, boolean upperCasePercentEncoding) {
        String iriSafeValue = makeSafe(toMakeIriSafe, upperCasePercentEncoding);
        assertThat(iriSafeValue, is(expectedResult));
    }

    private String makeSafe(String input, boolean upperCasePercentEncoding) {
        return upperCasePercentEncoding ? safeMaker.apply(input) : lcSafeMaker.apply(input);
    }
}
