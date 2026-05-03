package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.model.Mapping;
import io.carml.model.source.csvw.CsvwTable;
import java.nio.file.Path;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvwUrlResolverTest {

    @Nested
    class ResolveCsvwUrl {

        @Test
        void relativeUrlWithMappingBound_anchorsAgainstMappingDirectory(@TempDir Path tempDir) {
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("data.csv");
            var mapping = Mapping.builder()
                    .mappingFilePath(tempDir.resolve("mapping.ttl"))
                    .build();

            var resolved = CsvwUrlResolver.resolveCsvwUrl(csvwTable, mapping);

            assertThat(resolved, is(tempDir.resolve("data.csv").toString()));
        }

        @Test
        void relativeUrlWithoutMapping_returnsVerbatim() {
            // No Mapping context bound (e.g. introspector or test harness): fall back to the raw
            // URL so DuckDB's file_search_path / CWD takes over. Mirrors the FilePath fallback in
            // DuckDbFileSourceUtils.
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("data.csv");

            assertThat(CsvwUrlResolver.resolveCsvwUrl(csvwTable, null), is("data.csv"));
        }

        @Test
        void absoluteHttpUrl_returnsVerbatim() {
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("http://example.com/data.csv");

            assertThat(CsvwUrlResolver.resolveCsvwUrl(csvwTable, null), is("http://example.com/data.csv"));
        }

        @Test
        void absoluteHttpsUrl_returnsVerbatim(@TempDir Path tempDir) {
            // The mapping context is irrelevant when the URL already carries a scheme.
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("https://example.com/data.csv");
            var mapping = Mapping.builder()
                    .mappingFilePath(tempDir.resolve("mapping.ttl"))
                    .build();

            assertThat(CsvwUrlResolver.resolveCsvwUrl(csvwTable, mapping), is("https://example.com/data.csv"));
        }

        @Test
        void absoluteFileUrl_returnsVerbatim() {
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("file:///tmp/data.csv");

            assertThat(CsvwUrlResolver.resolveCsvwUrl(csvwTable, null), is("file:///tmp/data.csv"));
        }

        @Test
        void s3Url_returnsVerbatim() {
            // Any URL with a scheme bypasses anchoring, even non-http schemes that DuckDB knows
            // about (s3://, gs://, az://, hf://, ...).
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("s3://bucket/data.csv");

            assertThat(CsvwUrlResolver.resolveCsvwUrl(csvwTable, null), is("s3://bucket/data.csv"));
        }

        @Test
        void nullUrl_throws() {
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn(null);

            assertThrows(IllegalArgumentException.class, () -> CsvwUrlResolver.resolveCsvwUrl(csvwTable, null));
        }

        @Test
        void blankUrl_throws() {
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("  ");

            assertThrows(IllegalArgumentException.class, () -> CsvwUrlResolver.resolveCsvwUrl(csvwTable, null));
        }
    }
}
