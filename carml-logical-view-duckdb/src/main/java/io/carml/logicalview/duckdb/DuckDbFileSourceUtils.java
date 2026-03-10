package io.carml.logicalview.duckdb;

import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.Source;
import java.util.Locale;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Shared file-source utilities for DuckDB source handlers.
 *
 * <p>Extracts common file-resolution and format-detection logic used by
 * {@link JsonPathSourceHandler} and {@link CsvSourceHandler}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class DuckDbFileSourceUtils {

    private static final Set<String> PARQUET_EXTENSIONS = Set.of(".parquet", ".parq");

    /**
     * Resolves the file path from a {@link Source}, supporting both {@link FileSource} (URL-based)
     * and {@link FilePath} (path-based) source types.
     *
     * @param source the source to resolve
     * @return the resolved file path string
     * @throws IllegalArgumentException if the source is null, unsupported, or has no path/URL
     */
    static String resolveFilePath(Source source) {
        if (source instanceof FileSource fileSource) {
            var url = fileSource.getUrl();
            if (url == null || url.isBlank()) {
                throw new IllegalArgumentException("FileSource has no URL defined");
            }
            return url;
        }

        if (source instanceof FilePath filePath) {
            var path = filePath.getPath();
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("FilePath has no path defined");
            }
            return path;
        }

        if (source == null) {
            throw new IllegalArgumentException("LogicalSource has no source defined");
        }

        throw new IllegalArgumentException("Unsupported source type for file-based access: %s"
                .formatted(source.getClass().getName()));
    }

    /**
     * Checks whether the given file path points to a Parquet file based on its extension.
     *
     * @param filePath the file path to check
     * @return {@code true} if the file has a Parquet extension ({@code .parquet} or {@code .parq})
     */
    static boolean isParquetFile(String filePath) {
        var lowerPath = filePath.toLowerCase(Locale.ROOT);
        return PARQUET_EXTENSIONS.stream().anyMatch(lowerPath::endsWith);
    }

    /**
     * Checks whether the given source is a file-based source ({@link FilePath} or
     * {@link FileSource}).
     *
     * @param source the source to check
     * @return {@code true} if the source is file-based
     */
    static boolean isFileBasedSource(Source source) {
        return source instanceof FilePath || source instanceof FileSource;
    }
}
