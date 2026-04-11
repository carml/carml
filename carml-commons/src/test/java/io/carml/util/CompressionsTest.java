package io.carml.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CompressionsTest {

    private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

    private static final String RML_NS = "http://w3id.org/rml/";

    private static final IRI TARGZ = VF.createIRI(RML_NS, "targz");

    private static final IRI TARXZ = VF.createIRI(RML_NS, "tarxz");

    private static final IRI ZIP = VF.createIRI(RML_NS, "zip");

    private static final IRI GZIP = VF.createIRI(RML_NS, "gzip");

    private static final IRI NONE = VF.createIRI(RML_NS, "none");

    private static Stream<Arguments> validDecompression() {
        return Stream.of(
                Arguments.of("input.tar.gz", TARGZ),
                Arguments.of("input.tar.xz", TARXZ),
                Arguments.of("input.zip", ZIP),
                Arguments.of("input.csv.gz", GZIP),
                Arguments.of("input.csv", NONE),
                Arguments.of("input.csv", null),
                Arguments.of("input.csv", VF.createIRI(RML_NS, "unknown")));
    }

    @ParameterizedTest
    @MethodSource("validDecompression")
    void givenInputAndCompression_whenDecompress_thenReturnExpectedResult(String inputFileName, IRI compression)
            throws IOException {
        // Given
        var compressedInput = CompressionsTest.class.getResourceAsStream(inputFileName);

        // When
        var decompressed = Compressions.decompress(compressedInput, compression);

        // Then
        assertThat(IOUtils.toString(decompressed, StandardCharsets.UTF_8), startsWith("foo,bar,baz"));
    }

    private static Stream<Arguments> invalidDecompression() {
        return Stream.of(
                Arguments.of("input.tar.gz", TARXZ),
                Arguments.of("input.tar.xz", TARGZ),
                Arguments.of("input.zip", GZIP),
                Arguments.of("input.csv.gz", ZIP));
    }

    @ParameterizedTest
    @MethodSource("invalidDecompression")
    void givenInputAndInvalidCompression_whenDecompress_thenThrowException(String inputFileName, IRI compression) {
        // Given
        var compressedInput = CompressionsTest.class.getResourceAsStream(inputFileName);

        // When
        var exception =
                assertThrows(UncheckedIOException.class, () -> Compressions.decompress(compressedInput, compression));

        // Then
        assertThat(exception.getMessage(), startsWith("Could not open"));
    }
}
