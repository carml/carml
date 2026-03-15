package io.carml.jsonpath;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class JsonPathNormalizerTest {

    @Test
    void normalizeBracketNotation_simpleBracket_convertsToQuotedDot() {
        assertThat(JsonPathNormalizer.normalizeBracketNotation("$['Country Code']"), is("$.\"Country Code\""));
    }

    @Test
    void normalizeBracketNotation_chainedBrackets_convertsAll() {
        assertThat(JsonPathNormalizer.normalizeBracketNotation("$['key1']['key2']"), is("$.\"key1\".\"key2\""));
    }

    @Test
    void normalizeBracketNotation_mixedDotAndBracket_convertsBracketOnly() {
        assertThat(JsonPathNormalizer.normalizeBracketNotation("$.foo['bar']"), is("$.foo.\"bar\""));
    }

    @Test
    void normalizeBracketNotation_escapedBraces_unescapesRmlSequences() {
        assertThat(JsonPathNormalizer.normalizeBracketNotation("$['\\{Name\\}']"), is("$.\"{Name}\""));
    }

    @Test
    void normalizeBracketNotation_noBrackets_passesThrough() {
        assertThat(JsonPathNormalizer.normalizeBracketNotation("$.simple"), is("$.simple"));
    }

    @Test
    void normalizeBracketNotation_mixedBracketAndDotWithSpaces_convertsCorrectly() {
        assertThat(JsonPathNormalizer.normalizeBracketNotation("$['ISO 3166']"), is("$.\"ISO 3166\""));
    }

    @Test
    void normalizeBracketNotation_nestedBracketInDotPath_convertsCorrectly() {
        assertThat(
                JsonPathNormalizer.normalizeBracketNotation("$.data['my field'].value"),
                is("$.data.\"my field\".value"));
    }
}
