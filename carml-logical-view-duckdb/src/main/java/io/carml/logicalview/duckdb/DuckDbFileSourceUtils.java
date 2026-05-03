package io.carml.logicalview.duckdb;

import io.carml.logicalsourceresolver.sourceresolver.SourcePathResolver;
import io.carml.logicalsourceresolver.sourceresolver.SourceResolverException;
import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.Mapping;
import io.carml.model.Source;
import java.util.Locale;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the formulation-agnostic file-source shapes ({@link FilePath} and {@link FileSource})
 * to their effective DuckDB path or URL. Reference-formulation-specific source types (e.g.
 * {@code csvw:Table}) are handled by the matching {@link DuckDbSourceHandler} via
 * {@link DuckDbSourceHandler#resolveFilePath(io.carml.model.LogicalSource, Mapping)} so this util
 * stays free of formulation-specific knowledge.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class DuckDbFileSourceUtils {

    private static final Set<String> PARQUET_EXTENSIONS = Set.of(".parquet", ".parq");

    /**
     * Resolves the file path or URL string for {@code source}, delegating anchor-resolution
     * semantics to {@link SourcePathResolver#resolveAnchored} for {@link FilePath} sources so the
     * DuckDB and reactive evaluators reach identical effective paths for the same mapping
     * declaration. {@link FileSource} URLs are returned verbatim.
     *
     * <p>When a {@link FilePath} anchors against the mapping directory but no {@link Mapping} is
     * bound — for instance because the caller is the introspector or a test harness that wired
     * the DuckDB factory without a mapping reference — the resolver falls back to the raw
     * declared path and emits a one-time DEBUG line per source. DuckDB's {@code file_search_path}
     * or the JVM working directory then takes over, matching the legacy DuckDB behavior where
     * the string was passed through verbatim; test fixtures and CLI invocations that rely on
     * {@code file_search_path} resolution keep working.
     *
     * @param source the source to resolve; must be a {@link FilePath} or {@link FileSource}
     * @param mapping the active mapping context, used when {@code source} anchors against
     *     {@code rml:MappingDirectory}; may be {@code null}
     * @return absolute filesystem path string for {@link FilePath} whose anchor could be applied,
     *     the raw declared path when the anchor could not be applied, or the verbatim URL string
     *     for {@link FileSource}
     * @throws IllegalArgumentException if {@code source} is {@code null}, has no path / URL, or
     *     is of any unsupported type. Reference-formulation-specific sources (e.g. {@code
     *     csvw:Table}) are unsupported here; route them through the matching
     *     {@link DuckDbSourceHandler}.
     */
    static String resolveFilePath(Source source, Mapping mapping) {
        if (source == null) {
            throw new IllegalArgumentException("LogicalSource has no source defined");
        }
        if (source instanceof FileSource fileSource) {
            var url = fileSource.getUrl();
            if (url == null || url.isBlank()) {
                throw new IllegalArgumentException("FileSource has no URL defined");
            }
            return url;
        }
        if (!(source instanceof FilePath filePath)) {
            throw new IllegalArgumentException("Unsupported source type for formulation-agnostic file resolution: %s"
                    .formatted(source.getClass().getName()));
        }
        try {
            return SourcePathResolver.resolveAnchored(filePath, mapping);
        } catch (SourceResolverException e) {
            // Anchor resolution requires a Mapping context that is not available here. Fall back
            // to the raw declared path so DuckDB's file_search_path (or CWD) decides where to
            // look — the historical DuckDB behavior pre-dating anchored resolution. The DEBUG
            // line keeps the missing-mapping case observable without spamming production logs.
            var pathString = filePath.getPath();
            if (pathString != null && !pathString.isBlank()) {
                LOG.debug(
                        "FilePath source <{}> declares rml:root rml:MappingDirectory but no mapping context"
                                + " was supplied; falling back to raw rml:path \"{}\" for DuckDB to resolve.",
                        filePath.getResourceName(),
                        pathString);
                return pathString;
            }
            throw e;
        }
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
}
