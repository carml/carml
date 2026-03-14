package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
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

            assertThat(DuckDbFileSourceUtils.resolveFilePath(fileSource), is("data.json"));
        }

        @Test
        void filePath_returnsPath() {
            var filePath = mock(FilePath.class);
            when(filePath.getPath()).thenReturn("/tmp/data.csv");

            assertThat(DuckDbFileSourceUtils.resolveFilePath(filePath), is("/tmp/data.csv"));
        }

        @Test
        void fileSourceWithNullUrl_throws() {
            var fileSource = mock(FileSource.class);
            when(fileSource.getUrl()).thenReturn(null);

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(fileSource));
        }

        @Test
        void fileSourceWithBlankUrl_throws() {
            var fileSource = mock(FileSource.class);
            when(fileSource.getUrl()).thenReturn("  ");

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(fileSource));
        }

        @Test
        void filePathWithNullPath_throws() {
            var filePath = mock(FilePath.class);
            when(filePath.getPath()).thenReturn(null);

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(filePath));
        }

        @Test
        void filePathWithBlankPath_throws() {
            var filePath = mock(FilePath.class);
            when(filePath.getPath()).thenReturn("   ");

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(filePath));
        }

        @Test
        void nullSource_throws() {
            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(null));
        }

        @Test
        void unsupportedSourceType_throws() {
            var source = mock(Source.class);

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(source));
        }

        @Test
        void csvwTable_returnsUrl() {
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("data.csv");

            assertThat(DuckDbFileSourceUtils.resolveFilePath(csvwTable), is("data.csv"));
        }

        @Test
        void csvwTableWithNullUrl_throws() {
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn(null);

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(csvwTable));
        }

        @Test
        void csvwTableWithBlankUrl_throws() {
            var csvwTable = mock(CsvwTable.class);
            when(csvwTable.getUrl()).thenReturn("  ");

            assertThrows(IllegalArgumentException.class, () -> DuckDbFileSourceUtils.resolveFilePath(csvwTable));
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

    @Nested
    class IsFileBasedSource {

        @Test
        void filePath_returnsTrue() {
            var filePath = mock(FilePath.class);

            assertThat(DuckDbFileSourceUtils.isFileBasedSource(filePath), is(true));
        }

        @Test
        void fileSource_returnsTrue() {
            var fileSource = mock(FileSource.class);

            assertThat(DuckDbFileSourceUtils.isFileBasedSource(fileSource), is(true));
        }

        @Test
        void csvwTable_returnsTrue() {
            var csvwTable = mock(CsvwTable.class);

            assertThat(DuckDbFileSourceUtils.isFileBasedSource(csvwTable), is(true));
        }

        @Test
        void otherSource_returnsFalse() {
            var source = mock(Source.class);

            assertThat(DuckDbFileSourceUtils.isFileBasedSource(source), is(false));
        }

        @Test
        @SuppressWarnings("ConstantValue")
        void nullSource_returnsFalse() {
            assertThat(DuckDbFileSourceUtils.isFileBasedSource(null), is(false));
        }
    }
}
