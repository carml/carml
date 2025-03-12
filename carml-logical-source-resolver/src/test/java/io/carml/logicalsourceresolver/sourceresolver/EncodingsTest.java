package io.carml.logicalsourceresolver.sourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.carml.vocab.Rdf.Rml;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EncodingsTest {

    static Stream<Arguments> charsets() {
        return Stream.of(
                Arguments.of("given UTF-8 then return UTF-8", Rml.UTF_8, StandardCharsets.UTF_8),
                Arguments.of("given UTF-16 then return UTF-16", Rml.UTF_16, StandardCharsets.UTF_16),
                Arguments.of("given unknown encoding then return null", Rml.append, null),
                Arguments.of("given null then return null", null, null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("charsets")
    void resolveCharset(String testName, IRI encoding, Charset expected) {
        // When
        var actual = Encodings.resolveCharset(encoding);

        // Then
        assertThat(actual, is(Optional.ofNullable(expected)));
    }
}
