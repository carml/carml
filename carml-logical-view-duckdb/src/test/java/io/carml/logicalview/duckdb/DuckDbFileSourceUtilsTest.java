package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.Source;
import io.carml.model.source.csvw.CsvwTable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DuckDbFileSourceUtilsTest {

    @Nested
    class ResolveFilePath {

        @Test
        void fileSource_returnsUrl() {
            var fileSource = mock(FileSource.class);
            when(fileSource.getUrl()).thenReturn("data.json");

            assertThat(DuckDbFileSourceUtils.resolveFilePath(fileSource, null), is("data.json"));
        }

        @Test
        void filePath_returnsPath() {
            var filePath = mock(FilePath.class);
            when(filePath.getPath()).thenReturn("/tmp/data.csv");

            assertThat(DuckDbFileSourceUtils.resolveFilePath(filePath, null), is("/tmp/data.csv"));
        }

        @Test
        void fileSourceWithNullUrl_throws() {
            var fileSource = mock(FileSource.class);
            when(fileSource.getUrl()).thenReturn(null);

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(fileSource, null));
        }

        @Test
        void fileSourceWithBlankUrl_throws() {
            var fileSource = mock(FileSource.class);
            when(fileSource.getUrl()).thenReturn("  ");

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(fileSource, null));
        }

        @Test
        void filePathWithNullPath_throws() {
            var filePath = mock(FilePath.class);
            when(filePath.getPath()).thenReturn(null);

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(filePath, null));
        }

        @Test
        void filePathWithBlankPath_throws() {
            var filePath = mock(FilePath.class);
            when(filePath.getPath()).thenReturn("   ");

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(filePath, null));
        }

        @Test
        void nullSource_throws() {
            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(null, null));
        }

        @Test
        void unsupportedSourceType_throws() {
            var source = mock(Source.class);

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(source, null));
        }

        @Test
        void csvwTable_throws() {
            // The util only handles formulation-agnostic file shapes. CsvwTable carries CSVW
            // semantics that belong to the CSV source handler; passing it here signals a layering
            // bug, so the util refuses loudly instead of silently doing the wrong thing.
            var csvwTable = mock(CsvwTable.class);

            var exception = assertThrows(
                    IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(csvwTable, null));
            assertThat(exception.getMessage(), containsString("Unsupported source type"));
        }
    }

    @Nested
    class IsParquetFile {

        @Test
        void parquetExtension_returnsTrue() {
            assertThat(DuckDbFileSourceUtils.isParquetFile("data.parquet"), is(true));
        }

        @Test
        void parqExtension_returnsTrue() {
            assertThat(DuckDbFileSourceUtils.isParquetFile("data.parq"), is(true));
        }

        @Test
        void uppercaseParquet_returnsTrue() {
            assertThat(DuckDbFileSourceUtils.isParquetFile("DATA.PARQUET"), is(true));
        }

        @Test
        void csvExtension_returnsFalse() {
            assertThat(DuckDbFileSourceUtils.isParquetFile("data.csv"), is(false));
        }

        @Test
        void jsonExtension_returnsFalse() {
            assertThat(DuckDbFileSourceUtils.isParquetFile("data.json"), is(false));
        }
    }
}
