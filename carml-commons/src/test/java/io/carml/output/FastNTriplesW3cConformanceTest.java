package io.carml.output;

import static org.eclipse.rdf4j.model.util.Models.isomorphic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;

/**
 * Round-trip conformance tests using the official W3C N-Triples test suite. For each positive test
 * case:
 *
 * <ol>
 *   <li>Parses the W3C {@code .nt} file with Rio's {@code NTriplesParser} to get the expected
 *       model
 *   <li>Serializes that model with {@link FastNTriplesSerializer}
 *   <li>Parses our output back with Rio's {@code NTriplesParser}
 *   <li>Asserts the two models are isomorphic
 * </ol>
 *
 * <p>This verifies our serializer produces valid, semantically correct N-Triples per the W3C spec.
 *
 * @see <a href="https://www.w3.org/2013/N-TriplesTests/">W3C N-Triples Test Suite</a>
 */
class FastNTriplesW3cConformanceTest {

    private static final String TEST_RESOURCE_PATH = "/w3c/ntriples/";

    private final FastNTriplesSerializer serializer = FastNTriplesSerializer.withDefaults();

    static Stream<Arguments> w3cPositiveTestCases() {
        return Stream.of(
                Arguments.of("nt-syntax-file-01", "nt-syntax-file-01.nt"),
                Arguments.of("nt-syntax-file-02", "nt-syntax-file-02.nt"),
                Arguments.of("nt-syntax-file-03", "nt-syntax-file-03.nt"),
                Arguments.of("nt-syntax-uri-01", "nt-syntax-uri-01.nt"),
                Arguments.of("nt-syntax-uri-02", "nt-syntax-uri-02.nt"),
                Arguments.of("nt-syntax-uri-03", "nt-syntax-uri-03.nt"),
                Arguments.of("nt-syntax-uri-04", "nt-syntax-uri-04.nt"),
                Arguments.of("nt-syntax-string-01", "nt-syntax-string-01.nt"),
                Arguments.of("nt-syntax-string-02", "nt-syntax-string-02.nt"),
                Arguments.of("nt-syntax-string-03", "nt-syntax-string-03.nt"),
                Arguments.of("nt-syntax-str-esc-01", "nt-syntax-str-esc-01.nt"),
                Arguments.of("nt-syntax-str-esc-02", "nt-syntax-str-esc-02.nt"),
                Arguments.of("nt-syntax-str-esc-03", "nt-syntax-str-esc-03.nt"),
                Arguments.of("nt-syntax-bnode-01", "nt-syntax-bnode-01.nt"),
                Arguments.of("nt-syntax-bnode-02", "nt-syntax-bnode-02.nt"),
                Arguments.of("nt-syntax-bnode-03", "nt-syntax-bnode-03.nt"),
                Arguments.of("nt-syntax-datatypes-01", "nt-syntax-datatypes-01.nt"),
                Arguments.of("nt-syntax-datatypes-02", "nt-syntax-datatypes-02.nt"),
                Arguments.of("nt-syntax-subm-01", "nt-syntax-subm-01.nt"),
                Arguments.of("comment_following_triple", "comment_following_triple.nt"),
                Arguments.of("literal", "literal.nt"),
                Arguments.of("literal_all_controls", "literal_all_controls.nt"),
                Arguments.of("literal_all_punctuation", "literal_all_punctuation.nt"),
                Arguments.of("literal_ascii_boundaries", "literal_ascii_boundaries.nt"),
                Arguments.of("literal_with_2_dquotes", "literal_with_2_dquotes.nt"),
                Arguments.of("literal_with_2_squotes", "literal_with_2_squotes.nt"),
                Arguments.of("literal_with_BACKSPACE", "literal_with_BACKSPACE.nt"),
                Arguments.of("literal_with_CARRIAGE_RETURN", "literal_with_CARRIAGE_RETURN.nt"),
                Arguments.of("literal_with_CHARACTER_TABULATION", "literal_with_CHARACTER_TABULATION.nt"),
                Arguments.of("literal_with_dquote", "literal_with_dquote.nt"),
                Arguments.of("literal_with_FORM_FEED", "literal_with_FORM_FEED.nt"),
                Arguments.of("literal_with_LINE_FEED", "literal_with_LINE_FEED.nt"),
                Arguments.of("literal_with_numeric_escape4", "literal_with_numeric_escape4.nt"),
                Arguments.of("literal_with_numeric_escape8", "literal_with_numeric_escape8.nt"),
                Arguments.of("literal_with_REVERSE_SOLIDUS", "literal_with_REVERSE_SOLIDUS.nt"),
                Arguments.of("literal_with_REVERSE_SOLIDUS2", "literal_with_REVERSE_SOLIDUS2.nt"),
                Arguments.of("literal_with_squote", "literal_with_squote.nt"),
                Arguments.of("literal_with_UTF8_boundaries", "literal_with_UTF8_boundaries.nt"),
                Arguments.of("langtagged_string", "langtagged_string.nt"),
                Arguments.of("lantag_with_subtag", "lantag_with_subtag.nt"),
                Arguments.of("minimal_whitespace", "minimal_whitespace.nt"),
                Arguments.of("literal_false", "literal_false.nt"),
                Arguments.of("literal_true", "literal_true.nt"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("w3cPositiveTestCases")
    void roundTrip_w3cTestCase_producesIsomorphicModel(String testName, String fileName) throws IOException {
        // Parse W3C reference file
        var referenceModel = parseNTriples(getTestResource(fileName));

        // Skip empty test cases (files with only comments/whitespace)
        if (referenceModel.isEmpty()) {
            return;
        }

        // Serialize with FastNTriplesSerializer
        var output = new ByteArrayOutputStream();
        serializer.serialize(Flux.fromIterable(referenceModel), output);

        // Parse our output back
        var roundTripModel = parseNTriples(new ByteArrayInputStream(output.toByteArray()));

        // Assert isomorphic (handles blank node label differences)
        assertThat(
                "Round-trip of %s should produce isomorphic model".formatted(testName),
                isomorphic(roundTripModel, referenceModel),
                is(true));
    }

    private InputStream getTestResource(String fileName) {
        var resource = getClass().getResourceAsStream(TEST_RESOURCE_PATH + fileName);
        if (resource == null) {
            throw new IllegalStateException("Test resource not found: %s".formatted(TEST_RESOURCE_PATH + fileName));
        }
        return resource;
    }

    private static Model parseNTriples(InputStream input) throws IOException {
        return Rio.parse(input, RDFFormat.NTRIPLES);
    }
}
