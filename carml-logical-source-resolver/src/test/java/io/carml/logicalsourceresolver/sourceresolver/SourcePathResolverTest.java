package io.carml.logicalsourceresolver.sourceresolver;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.Mapping;
import io.carml.model.Source;
import io.carml.model.impl.CarmlFilePath;
import io.carml.vocab.Rdf.Rml;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourcePathResolverTest {

    @BeforeEach
    void clearWarnState() throws Exception {
        // Clear the static dedup set so WARN-emission tests start from a known baseline.
        warnedSet().clear();
    }

    private static Set<?> warnedSet() throws Exception {
        var field = SourcePathResolver.class.getDeclaredField("WARNED_ABSOLUTE_WITH_ROOT");
        field.setAccessible(true);
        return (Set<?>) field.get(null);
    }

    @Nested
    class ResolveAnchoredFilePath {

        @Test
        void absolutePath_returnsAsIs(@TempDir Path tempDir) {
            var source = CarmlFilePath.builder().path(tempDir.toString()).build();

            assertThat(SourcePathResolver.resolveAnchored(source, null), is(tempDir.toString()));
        }

        @Test
        void relativePathWithWorkingDirectoryRoot_resolvesToWorkingDirectory() {
            var source = CarmlFilePath.builder()
                    .path("src/test/resources/emptyBaseInput.csv")
                    .root(Rml.CurrentWorkingDirectory)
                    .build();

            var resolved =
                    SourcePathResolver.resolveAnchored(source, Mapping.builder().build());

            assertThat(resolved, endsWith("src/test/resources/emptyBaseInput.csv"));
        }

        @Test
        void relativePathWithMappingDirectoryRoot_resolvesAgainstMappingDirectory(@TempDir Path tempDir) {
            var source = CarmlFilePath.builder()
                    .path("data.csv")
                    .root(Rml.MappingDirectory)
                    .build();
            var mapping = Mapping.builder()
                    .mappingFilePath(tempDir.resolve("mapping.ttl"))
                    .build();

            var resolved = SourcePathResolver.resolveAnchored(source, mapping);

            assertThat(resolved, is(tempDir.resolve("data.csv").toString()));
        }

        @Test
        void relativePathWithoutRoot_defaultsToWorkingDirectory() {
            var source = CarmlFilePath.builder().path("data.csv").build();

            var resolved = SourcePathResolver.resolveAnchored(source, null);

            assertThat(resolved, is("data.csv"));
        }

        @Test
        void relativePathWithUnrecognizedRoot_defaultsToWorkingDirectory() {
            var unrecognizedRoot = SimpleValueFactory.getInstance().createIRI("http://example.com/unknown-root");
            var source = CarmlFilePath.builder()
                    .path("data.csv")
                    .root(unrecognizedRoot)
                    .build();

            var resolved = SourcePathResolver.resolveAnchored(source, null);

            assertThat(resolved, is("data.csv"));
        }

        @Test
        void filePathWithoutPath_throws() {
            var source = CarmlFilePath.builder().build();

            assertThrows(IllegalArgumentException.class, () -> SourcePathResolver.resolveAnchored(source, null));
        }

        @Test
        void filePathWithBlankPath_throws() {
            var source = CarmlFilePath.builder().path("   ").build();

            assertThrows(IllegalArgumentException.class, () -> SourcePathResolver.resolveAnchored(source, null));
        }
    }

    @Nested
    class ResolveAnchoredErrorCases {

        @Test
        void nullSource_throws() {
            assertThrows(IllegalArgumentException.class, () -> SourcePathResolver.resolveAnchored(null, null));
        }

        @Test
        void unsupportedSourceType_throws() {
            var source = mock(Source.class);

            var exception = assertThrows(
                    IllegalArgumentException.class, () -> SourcePathResolver.resolveAnchored(source, null));
            assertThat(exception.getMessage(), startsWith("Unsupported source type"));
        }

        @Test
        void fileSource_isUnsupported() {
            // Reference-formulation-agnostic file URLs (FileSource) belong to the caller (e.g.
            // DuckDbFileSourceUtils). SourcePathResolver only handles FilePath and refuses
            // everything else loudly.
            var source = mock(FileSource.class);

            var exception = assertThrows(
                    IllegalArgumentException.class, () -> SourcePathResolver.resolveAnchored(source, null));
            assertThat(exception.getMessage(), startsWith("Unsupported source type"));
        }
    }

    @Nested
    class ResolveAnchoredPath {

        @Test
        void filePath_returnsPath(@TempDir Path tempDir) {
            var source = CarmlFilePath.builder()
                    .path("data.csv")
                    .root(Rml.MappingDirectory)
                    .build();
            var mapping = Mapping.builder()
                    .mappingFilePath(tempDir.resolve("mapping.ttl"))
                    .build();

            assertThat(SourcePathResolver.resolveAnchoredPath(source, mapping), is(tempDir.resolve("data.csv")));
        }

        @Test
        void unsupportedSource_throws() {
            var source = mock(Source.class);

            var exception = assertThrows(
                    IllegalArgumentException.class, () -> SourcePathResolver.resolveAnchoredPath(source, null));
            assertThat(exception.getMessage(), startsWith("Unsupported source type"));
        }
    }

    @Nested
    class ResolveMappingDirectory {

        @Test
        void emptyMappingFilePaths_throws() {
            var source = CarmlFilePath.builder()
                    .path("data.csv")
                    .root(Rml.MappingDirectory)
                    .build();
            var mapping = Mapping.builder().build();

            var exception = assertThrows(
                    SourceResolverException.class, () -> SourcePathResolver.resolveMappingDirectory(source, mapping));
            assertThat(exception.getMessage(), startsWith("No mapping file paths provided for source"));
        }

        @Test
        void multipleMappingDirectories_throws(@TempDir Path tempDir, @TempDir Path tempDir2) {
            var source = CarmlFilePath.builder()
                    .path("data.csv")
                    .root(Rml.MappingDirectory)
                    .build();
            var mapping = Mapping.builder()
                    .mappingFilePath(tempDir.resolve("mapping.ttl"))
                    .mappingFilePath(tempDir2.resolve("mapping.ttl"))
                    .build();

            var exception = assertThrows(
                    SourceResolverException.class, () -> SourcePathResolver.resolveMappingDirectory(source, mapping));
            assertThat(
                    exception.getMessage(),
                    startsWith("Multiple mapping directories found, where only one was expected, for source"));
        }

        @Test
        void mappingFileAtFilesystemRoot_resolvesAgainstRoot() {
            var source = CarmlFilePath.builder()
                    .path("data.csv")
                    .root(Rml.MappingDirectory)
                    .build();
            // A bare "mapping.ttl" Path has a null parent — used to simulate a mapping file at
            // the filesystem root without actually touching the host filesystem.
            var mapping =
                    Mapping.builder().mappingFilePath(Path.of("mapping.ttl")).build();

            var resolved = SourcePathResolver.resolveMappingDirectory(source, mapping);

            assertThat(resolved, is(Path.of("/")));
        }

        @Test
        void singleMappingFilePath_returnsParentDirectory(@TempDir Path tempDir) {
            // Direct test of the public helper used by reference-formulation-specific callers
            // (e.g. CSVW URL resolution in carml-logical-view-duckdb).
            var source = mock(Source.class);
            var mapping = Mapping.builder()
                    .mappingFilePath(tempDir.resolve("mapping.ttl"))
                    .build();

            var resolved = SourcePathResolver.resolveMappingDirectory(source, mapping);

            assertThat(resolved, is(tempDir));
        }

        @Test
        void nullMapping_throws() {
            var source = mock(Source.class);

            var exception = assertThrows(
                    SourceResolverException.class, () -> SourcePathResolver.resolveMappingDirectory(source, null));
            assertThat(exception.getMessage(), containsString("No mapping file paths"));
        }
    }

    @Nested
    class WarnDeduplication {
        // The dedup behaviour is verified through the static WARNED_ABSOLUTE_WITH_ROOT set rather
        // than by capturing log output. slf4j-simple binds System.err at static-init time, so
        // mid-test System.setErr(...) does not actually intercept the WARN line — checking the
        // post-call state of the dedup set is both stable and direct.

        @Test
        void absolutePathWithRoot_addsSingleEntryForSameSource(@TempDir Path tempDir) throws Exception {
            var source = CarmlFilePath.builder()
                    .path(tempDir.toString())
                    .root(Rml.MappingDirectory)
                    .build();

            SourcePathResolver.resolveAnchored(source, null);
            SourcePathResolver.resolveAnchored(source, null);

            assertThat(warnedSet().size(), is(1));
        }

        @Test
        void absolutePathWithoutRoot_addsNoEntry(@TempDir Path tempDir) throws Exception {
            var source = CarmlFilePath.builder().path(tempDir.toString()).build();

            SourcePathResolver.resolveAnchored(source, null);

            assertThat(warnedSet().size(), is(0));
        }

        @Test
        void blankNodeFilePath_dedupsByIdentity_addsOneEntryPerInstance(@TempDir Path tempDir) throws Exception {
            // Two FilePath instances with identical absolute path + root, but distinct identity.
            // dedupKeyFor falls back to System.identityHashCode for sources whose getResourceName()
            // returns null/blank, so each instance should produce its own dedup-set entry.
            var firstBnode = mockBlankNodeFilePath(tempDir);
            var secondBnode = mockBlankNodeFilePath(tempDir);

            SourcePathResolver.resolveAnchored(firstBnode, null);
            SourcePathResolver.resolveAnchored(firstBnode, null); // same instance — dedups
            SourcePathResolver.resolveAnchored(secondBnode, null); // different instance — new entry

            assertThat(warnedSet().size(), is(2));
        }

        private FilePath mockBlankNodeFilePath(Path tempDir) {
            var filePath = mock(FilePath.class);
            lenient().when(filePath.getPath()).thenReturn(tempDir.toString());
            lenient().when(filePath.getRoot()).thenReturn(Rml.MappingDirectory);
            // Return null for getResourceName() to force the bnode-fallback dedup branch.
            lenient().when(filePath.getResourceName()).thenReturn(null);
            return filePath;
        }
    }

    @Nested
    class HappyPathDataReadable {

        /**
         * End-to-end check that a relative path anchored at the mapping directory points to the
         * expected file on disk: the resolver returns the right string AND the file is actually
         * accessible via that string. Catches accidental string-mangling regressions.
         */
        @Test
        void mappingDirectoryAnchor_resolvesToReadableFile(@TempDir Path tempDir) throws Exception {
            Files.writeString(tempDir.resolve("data.csv"), "id,name\n1,alice\n", StandardCharsets.UTF_8);
            var source = CarmlFilePath.builder()
                    .path("data.csv")
                    .root(Rml.MappingDirectory)
                    .build();
            var mapping = Mapping.builder()
                    .mappingFilePath(tempDir.resolve("mapping.ttl"))
                    .build();

            var resolved = SourcePathResolver.resolveAnchoredPath(source, mapping);

            assertThat(Files.exists(resolved), is(true));
            assertThat(Files.readString(resolved, StandardCharsets.UTF_8), startsWith("id,name"));
        }
    }
}
