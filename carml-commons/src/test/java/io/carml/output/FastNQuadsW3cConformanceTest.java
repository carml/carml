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
 * Round-trip conformance tests using the official W3C N-Quads test suite. For each positive test
 * case:
 *
 * <ol>
 *   <li>Parses the W3C {@code .nq} file with Rio's {@code NQuadsParser} to get the expected model
 *   <li>Serializes that model with {@link FastNQuadsSerializer}
 *   <li>Parses our output back with Rio's {@code NQuadsParser}
 *   <li>Asserts the two models are isomorphic
 * </ol>
 *
 * <p>This verifies our serializer produces valid, semantically correct N-Quads per the W3C spec.
 *
 * @see <a href="https://www.w3.org/2013/N-QuadsTests/">W3C N-Quads Test Suite</a>
 */
class FastNQuadsW3cConformanceTest {

    private static final String TEST_RESOURCE_PATH = "/w3c/nquads/";

    private final FastNQuadsSerializer serializer = FastNQuadsSerializer.withDefaults();

    static Stream<Arguments> w3cPositiveTestCases() {
        return Stream.of(
                // N-Quads specific tests with graph context (IRI graphs)
                Arguments.of("nq-syntax-uri-01", "nq-syntax-uri-01.nq"),
                Arguments.of("nq-syntax-uri-02", "nq-syntax-uri-02.nq"),
                Arguments.of("nq-syntax-uri-03", "nq-syntax-uri-03.nq"),
                Arguments.of("nq-syntax-uri-04", "nq-syntax-uri-04.nq"),
                Arguments.of("nq-syntax-uri-05", "nq-syntax-uri-05.nq"),
                Arguments.of("nq-syntax-uri-06", "nq-syntax-uri-06.nq"),
                // N-Quads specific tests with graph context (BNode graphs)
                Arguments.of("nq-syntax-bnode-01", "nq-syntax-bnode-01.nq"),
                Arguments.of("nq-syntax-bnode-02", "nq-syntax-bnode-02.nq"),
                Arguments.of("nq-syntax-bnode-03", "nq-syntax-bnode-03.nq"),
                Arguments.of("nq-syntax-bnode-04", "nq-syntax-bnode-04.nq"),
                Arguments.of("nq-syntax-bnode-05", "nq-syntax-bnode-05.nq"),
                Arguments.of("nq-syntax-bnode-06", "nq-syntax-bnode-06.nq"),
                // N-Triples compatible tests (no graph context)
                Arguments.of("nt-syntax-file-01", "nt-syntax-file-01.nq"),
                Arguments.of("nt-syntax-file-02", "nt-syntax-file-02.nq"),
                Arguments.of("nt-syntax-file-03", "nt-syntax-file-03.nq"),
                Arguments.of("nt-syntax-uri-01", "nt-syntax-uri-01.nq"),
                Arguments.of("nt-syntax-uri-02", "nt-syntax-uri-02.nq"),
                Arguments.of("nt-syntax-uri-03", "nt-syntax-uri-03.nq"),
                Arguments.of("nt-syntax-uri-04", "nt-syntax-uri-04.nq"),
                Arguments.of("nt-syntax-string-01", "nt-syntax-string-01.nq"),
                Arguments.of("nt-syntax-string-02", "nt-syntax-string-02.nq"),
                Arguments.of("nt-syntax-string-03", "nt-syntax-string-03.nq"),
                Arguments.of("nt-syntax-str-esc-01", "nt-syntax-str-esc-01.nq"),
                Arguments.of("nt-syntax-str-esc-02", "nt-syntax-str-esc-02.nq"),
                Arguments.of("nt-syntax-str-esc-03", "nt-syntax-str-esc-03.nq"),
                Arguments.of("nt-syntax-bnode-01", "nt-syntax-bnode-01.nq"),
                Arguments.of("nt-syntax-bnode-02", "nt-syntax-bnode-02.nq"),
                Arguments.of("nt-syntax-bnode-03", "nt-syntax-bnode-03.nq"),
                Arguments.of("nt-syntax-datatypes-01", "nt-syntax-datatypes-01.nq"),
                Arguments.of("nt-syntax-datatypes-02", "nt-syntax-datatypes-02.nq"),
                Arguments.of("nt-syntax-subm-01", "nt-syntax-subm-01.nq"),
                // Literal tests
                Arguments.of("comment_following_triple", "comment_following_triple.nq"),
                Arguments.of("literal", "literal.nq"),
                Arguments.of("literal_all_controls", "literal_all_controls.nq"),
                Arguments.of("literal_all_punctuation", "literal_all_punctuation.nq"),
                Arguments.of("literal_ascii_boundaries", "literal_ascii_boundaries.nq"),
                Arguments.of("literal_with_2_dquotes", "literal_with_2_dquotes.nq"),
                Arguments.of("literal_with_2_squotes", "literal_with_2_squotes.nq"),
                Arguments.of("literal_with_BACKSPACE", "literal_with_BACKSPACE.nq"),
                Arguments.of("literal_with_CARRIAGE_RETURN", "literal_with_CARRIAGE_RETURN.nq"),
                Arguments.of("literal_with_CHARACTER_TABULATION", "literal_with_CHARACTER_TABULATION.nq"),
                Arguments.of("literal_with_dquote", "literal_with_dquote.nq"),
                Arguments.of("literal_with_FORM_FEED", "literal_with_FORM_FEED.nq"),
                Arguments.of("literal_with_LINE_FEED", "literal_with_LINE_FEED.nq"),
                Arguments.of("literal_with_numeric_escape4", "literal_with_numeric_escape4.nq"),
                Arguments.of("literal_with_numeric_escape8", "literal_with_numeric_escape8.nq"),
                Arguments.of("literal_with_REVERSE_SOLIDUS", "literal_with_REVERSE_SOLIDUS.nq"),
                Arguments.of("literal_with_REVERSE_SOLIDUS2", "literal_with_REVERSE_SOLIDUS2.nq"),
                Arguments.of("literal_with_squote", "literal_with_squote.nq"),
                Arguments.of("literal_with_UTF8_boundaries", "literal_with_UTF8_boundaries.nq"),
                Arguments.of("langtagged_string", "langtagged_string.nq"),
                Arguments.of("lantag_with_subtag", "lantag_with_subtag.nq"),
                Arguments.of("minimal_whitespace", "minimal_whitespace.nq"),
                Arguments.of("literal_false", "literal_false.nq"),
                Arguments.of("literal_true", "literal_true.nq"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("w3cPositiveTestCases")
    void roundTrip_w3cTestCase_producesIsomorphicModel(String testName, String fileName) throws IOException {
        // Parse W3C reference file
        var referenceModel = parseNQuads(getTestResource(fileName));

        // Skip empty test cases (files with only comments/whitespace)
        if (referenceModel.isEmpty()) {
            return;
        }

        // Serialize with FastNQuadsSerializer
        var output = new ByteArrayOutputStream();
        serializer.serialize(Flux.fromIterable(referenceModel), output);

        // Parse our output back
        var roundTripModel = parseNQuads(new ByteArrayInputStream(output.toByteArray()));

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

    private static Model parseNQuads(InputStream input) throws IOException {
        return Rio.parse(input, RDFFormat.NQUADS);
    }
}
