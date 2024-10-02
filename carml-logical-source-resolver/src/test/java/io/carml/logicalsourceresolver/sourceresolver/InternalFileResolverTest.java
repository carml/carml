package io.carml.logicalsourceresolver.sourceresolver;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.model.Mapping;
import io.carml.model.impl.CarmlDcatDistribution;
import io.carml.model.impl.CarmlRelativePathSource;
import io.carml.vocab.Rdf.Rml;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class InternalFileResolverTest {

    @Test
    void givenPathStringAndBasePath_whenResolve_thenReturnExpected(@TempDir Path basePath) {
        // Given
        createTestFile(basePath, "input.csv");
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .basePath(basePath)
                .pathString("input.csv")
                .build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("foo,bar,baz"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void givenPathStringAndClassPathBase_whenResolve_thenReturnExpected() {
        // Given
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .classPathBase("io/carml/logicalsourceresolver/sourceresolver/")
                .pathString("input.csv")
                .build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("foo,bar,baz"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void givenPathStringAndEmptyClassPathBase_whenResolve_thenReturnExpected() {
        // Given
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .classPathBase("")
                .pathString("emptyBaseInput.csv")
                .build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("baz,bar,foo"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void givenPathStringAndClassPathBaseAndLoadingClass_whenResolve_thenReturnExpected() {
        // Given
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .classPathBase("sourceresolver")
                .pathString("input.csv")
                .loadingClass(ResolvedSource.class)
                .build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("foo,bar,baz"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("ReactiveStreamsUnusedPublisher")
    void givenPathStringAndClassPathBaseThatDoesNotExist_whenResolve_thenThrowSourceResolverException() {
        // Given
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .classPathBase("foo")
                .pathString("input.csv")
                .build();

        // When
        var exception = assertThrows(SourceResolverException.class, () -> resolver.resolve(source, mapping));

        // Then
        assertThat(exception.getMessage(), startsWith("Could not resolve classpath resource foo/input.csv for source"));
    }

    @Test
    void givenPathStringAndPathRelativeToMappingDir_whenResolve_thenReturnExpected(@TempDir Path tempDir) {
        // Given
        createTestFile(tempDir, "input.csv");
        var source =
                CarmlRelativePathSource.builder().root(Rml.MappingDirectory).build();
        var mapping = Mapping.builder()
                .mappingFilePath(tempDir.resolve("mapping.ttl"))
                .build();
        var resolver = InternalFileResolver.builder()
                .pathString("input.csv")
                .pathRelativeTo(PathRelativeTo.MAPPING_DIRECTORY)
                .build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("foo,bar,baz"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("ReactiveStreamsUnusedPublisher")
    void givenPathStringAndPathRelativeToMappingDirButNoMappingPath_whenResolve_thenThrowException(
            @TempDir Path tempDir, @TempDir Path tempDir2) {
        // Given
        var source =
                CarmlRelativePathSource.builder().root(Rml.MappingDirectory).build();
        var mapping = Mapping.builder()
                .mappingFilePath(tempDir.resolve("mapping.ttl"))
                .mappingFilePath(tempDir2.resolve("mapping.ttl"))
                .build();
        var resolver = InternalFileResolver.builder()
                .pathString("input.csv")
                .pathRelativeTo(PathRelativeTo.MAPPING_DIRECTORY)
                .build();

        // When
        var exception = assertThrows(SourceResolverException.class, () -> resolver.resolve(source, mapping));

        // Then
        assertThat(
                exception.getMessage(),
                startsWith("Multiple mapping directories found, where only one was expected, for source"));
    }

    @Test
    @SuppressWarnings("ReactiveStreamsUnusedPublisher")
    void givenPathStringAndPathRelativeToMappingDirWithMultipleMappingPath_whenResolve_thenThrowException() {
        // Given
        var source =
                CarmlRelativePathSource.builder().root(Rml.MappingDirectory).build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .pathString("input.csv")
                .pathRelativeTo(PathRelativeTo.MAPPING_DIRECTORY)
                .build();

        // When
        var exception = assertThrows(SourceResolverException.class, () -> resolver.resolve(source, mapping));

        // Then
        assertThat(exception.getMessage(), startsWith("No mapping file paths provided for source"));
    }

    @Test
    void givenPathStringAndPathRelativeToWorkingDir_whenResolve_thenReturnExpected() {
        // Given
        var source = CarmlRelativePathSource.builder()
                .root(Rml.CurrentWorkingDirectory)
                .build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .pathString("src/test/resources/emptyBaseInput.csv")
                .pathRelativeTo(PathRelativeTo.WORKING_DIRECTORY)
                .build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("baz,bar,foo"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void givenPathStringOnly_whenResolve_thenReturnExpected() {
        // Given
        var source = CarmlRelativePathSource.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .pathString("/src/test/resources/emptyBaseInput.csv")
                .build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("baz,bar,foo"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("ReactiveStreamsUnusedPublisher")
    void givenPathStringAndBasePath_whenResolveAndNotExists_thenThrowException(@TempDir Path basePath) {
        // Given
        createTestFile(basePath, "input.csv");
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .basePath(basePath)
                .pathString("input2.csv")
                .build();

        // When
        var exception = assertThrows(SourceResolverException.class, () -> resolver.resolve(source, mapping));

        // Then
        assertThat(exception.getMessage(), startsWith("File does not exist at path"));
    }

    @Test
    void givenPathStringAndClassPathBaseAndCompression_whenResolve_thenReturnExpected() {
        // Given
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .classPathBase("io/carml/logicalsourceresolver/sourceresolver")
                .pathString("input.zip")
                .compression(Rml.zip)
                .build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("foo,bar,baz"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void givenFileUrl_whenResolve_thenReturnExpected(@TempDir Path basePath) throws MalformedURLException {
        // Given
        var createdFile = createTestFile(basePath, "input.csv");
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver =
                InternalFileResolver.builder().url(createdFile.toUri().toURL()).build();

        // When
        var inputStreamMono = resolver.resolve(source, mapping);

        // Then
        StepVerifier.create(inputStreamMono)
                .expectNextMatches(inputStream -> {
                    try {
                        var actual = IOUtils.toString(inputStream, UTF_8);
                        assertThat(actual, startsWith("foo,bar,baz"));
                        return true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void givenWebUrl_whenResolve_thenReturnExpected() throws MalformedURLException {
        // Given
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder()
                .url(new URL("https://example.foo/bar.csv"))
                .build();

        try (MockedStatic<GetHttpUrl> getHttpUrlMockedStatic = Mockito.mockStatic(GetHttpUrl.class)) {
            var getHttpUrl = mock(GetHttpUrl.class);
            getHttpUrlMockedStatic.when(GetHttpUrl::getInstance).thenReturn(getHttpUrl);

            when(getHttpUrl.apply(new URL("https://example.foo/bar.csv")))
                    .thenReturn(Mono.just(IOUtils.toInputStream("foo,bar,baz", UTF_8)));

            // When
            var inputStreamMono = resolver.resolve(source, mapping);

            // Then
            StepVerifier.create(inputStreamMono)
                    .expectNextMatches(inputStream -> {
                        try {
                            var actual = IOUtils.toString(inputStream, UTF_8);
                            assertThat(actual, startsWith("foo,bar,baz"));
                            return true;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .verifyComplete();
        }
    }

    @Test
    @SuppressWarnings("ReactiveStreamsUnusedPublisher")
    void givenUrlWithUnsupportedProtocol_whenResolve_thenThrowException() throws MalformedURLException {
        // Given
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver =
                InternalFileResolver.builder().url(new URL("ftp://foo/bar")).build();

        // When
        var exception = assertThrows(SourceResolverException.class, () -> resolver.resolve(source, mapping));

        // Then
        assertThat(exception.getMessage(), startsWith("Unsupported protocol ftp for source"));
    }

    @Test
    @SuppressWarnings("ReactiveStreamsUnusedPublisher")
    void givenNoPathStringOrUrl_whenResolve_thenThrowException() {
        // Given
        var source = CarmlDcatDistribution.builder().build();
        var mapping = Mapping.builder().build();
        var resolver = InternalFileResolver.builder().build();

        // When
        var exception = assertThrows(SourceResolverException.class, () -> resolver.resolve(source, mapping));

        // Then
        assertThat(exception.getMessage(), startsWith("No path or URL provided for source"));
    }

    private Path createTestFile(Path path, String resourceLocation) {
        try {
            var testInput = Files.readAllBytes(Paths.get(
                    InternalFileResolverTest.class.getResource(resourceLocation).getFile()));

            var outputPath = path.resolve(resourceLocation);
            Files.write(outputPath, testInput);

            return outputPath;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
