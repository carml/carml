package io.carml.logicalsourceresolver.sourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.carml.vocab.Rdf.Rml;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CompressionsTest {

    private static Stream<Arguments> validDecompression() {
        return Stream.of(
                Arguments.of("input.tar.gz", Rml.targz),
                Arguments.of("input.tar.xz", Rml.tarxz),
                Arguments.of("input.zip", Rml.zip),
                Arguments.of("input.csv.gz", Rml.gzip),
                Arguments.of("input.csv", Rml.none),
                Arguments.of("input.csv", null));
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
                Arguments.of("input.tar.gz", Rml.tarxz),
                Arguments.of("input.tar.xz", Rml.targz),
                Arguments.of("input.zip", Rml.gzip),
                Arguments.of("input.csv.gz", Rml.zip));
    }

    @ParameterizedTest
    @MethodSource("invalidDecompression")
    void givenInputAndInvalidCompression_whenDecompress_thenThrowException(String inputFileName, IRI compression) {
        // Given
        var compressedInput = CompressionsTest.class.getResourceAsStream(inputFileName);

        // When
        var exception = assertThrows(
                SourceResolverException.class, () -> Compressions.decompress(compressedInput, compression));

        // Then
        assertThat(exception.getMessage(), startsWith("Could not open"));
    }
}
