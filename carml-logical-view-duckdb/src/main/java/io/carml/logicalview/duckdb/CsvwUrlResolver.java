package io.carml.logicalview.duckdb;

import io.carml.logicalsourceresolver.sourceresolver.SourcePathResolver;
import io.carml.model.Mapping;
import io.carml.model.source.csvw.CsvwTable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the effective file path or URL for a {@link CsvwTable} source, applying the CSVW-spec
 * rule that a relative {@code csvw:url} anchors against the parent directory of the mapping file.
 *
 * <p>Lives in {@code carml-logical-view-duckdb} so reference-formulation-specific knowledge stays
 * out of the generic {@link SourcePathResolver}. Both {@link CsvSourceHandler} (for SQL compile)
 * and {@link DuckDbLogicalViewEvaluator} (for source-file existence validation) call this resolver
 * to reach the same effective path.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class CsvwUrlResolver {

    private static final String URL_SCHEME_REGEX = "^[a-zA-Z][a-zA-Z0-9+.-]*://.*";

    /**
     * Resolves the URL of {@code csvwTable} to an absolute file path string when relative, or
     * returns it verbatim when it already carries a scheme ({@code http:}, {@code file:}, ...).
     *
     * <p>When the URL is relative but no {@link Mapping} is bound — e.g. the introspector or a
     * test harness wired the DuckDB factory without a mapping reference — the raw URL is returned
     * verbatim and a one-time DEBUG line is emitted. DuckDB's {@code file_search_path} (or the JVM
     * working directory) then takes over, mirroring the legacy behavior.
     *
     * @param csvwTable the source whose URL should be resolved; must carry a non-blank URL
     * @param mapping the active mapping context, used to anchor relative URLs against the mapping
     *     directory; may be {@code null}
     * @return the absolute file path string for a successfully anchored relative URL, the verbatim
     *     URL string when the URL already carries a scheme, or the verbatim URL when no mapping
     *     context is bound
     * @throws IllegalArgumentException if {@code csvwTable} has no URL defined
     */
    static String resolveCsvwUrl(CsvwTable csvwTable, Mapping mapping) {
        var url = csvwTable.getUrl();
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("CsvwTable has no URL defined");
        }
        if (looksLikeUrl(url)) {
            return url;
        }
        if (mapping == null) {
            LOG.debug(
                    "CsvwTable source <{}> declares relative csvw:url \"{}\" but no mapping context"
                            + " was supplied; falling back to raw csvw:url for DuckDB to resolve.",
                    csvwTable.getResourceName(),
                    url);
            return url;
        }
        return SourcePathResolver.resolveMappingDirectory(csvwTable, mapping)
                .resolve(url)
                .toString();
    }

    private static boolean looksLikeUrl(String url) {
        return url.matches(URL_SCHEME_REGEX);
    }
}
