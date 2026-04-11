package io.carml.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EncodingsTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final IRI UTF_8 = VF.createIRI(RML_NS, "UTF-8");

    private static final IRI UTF_16 = VF.createIRI(RML_NS, "UTF-16");

    private static final IRI UNKNOWN = VF.createIRI(RML_NS, "append");

    static Stream<Arguments> charsets() {
        return Stream.of(
                Arguments.of("given UTF-8 then return UTF-8", UTF_8, StandardCharsets.UTF_8),
                Arguments.of("given UTF-16 then return UTF-16", UTF_16, StandardCharsets.UTF_16),
                Arguments.of("given unknown encoding then return null", UNKNOWN, null),
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
