package io.carml.logicalsourceresolver.sourceresolver;

import static io.carml.util.LogUtil.exception;

import io.carml.model.FilePath;
import io.carml.model.Mapping;
import io.carml.model.Resource;
import io.carml.model.Source;
import io.carml.vocab.Rdf.Rml;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

/**
 * Shared resolver for {@link FilePath} sources and a generic mapping-directory lookup.
 *
 * <p><strong>Anchor semantics</strong>: absolute {@code rml:path} values are returned as-is and
 * {@code rml:root} is unused for absolute paths. Relative {@code rml:path} values resolve against
 * the anchor named by {@code rml:root} — {@code rml:MappingDirectory} resolves against the parent
 * directory of the mapping file (read from {@link Mapping#getMappingFilePaths()});
 * {@code rml:CurrentWorkingDirectory}, no {@code rml:root}, and any unrecognized root IRI all
 * default to the JVM working directory.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SourcePathResolver {

    /**
     * Tracks the source IRIs (or hash-coded references for blank nodes) that have already produced
     * the absolute-path-with-root WARN, so it fires once per source rather than once per
     * evaluation.
     */
    private static final Set<String> WARNED_ABSOLUTE_WITH_ROOT = ConcurrentHashMap.newKeySet();

    /**
     * Resolves the effective filesystem path string for a {@link FilePath} source. See the class
     * Javadoc for anchor semantics.
     *
     * @param source the source to resolve; must be a non-null {@link FilePath}
     * @param mapping the mapping context, used to locate the mapping directory when {@code source}
     *     anchors a relative path against {@code rml:MappingDirectory}; may be {@code null} when
     *     no mapping directory is needed
     * @return absolute filesystem path string for the resolved {@link FilePath}
     * @throws IllegalArgumentException if {@code source} is {@code null}, has no path, or is of
     *     any type other than {@link FilePath}
     * @throws SourceResolverException if {@code source} anchors against
     *     {@code rml:MappingDirectory} but the supplied {@link Mapping} has no resolvable mapping
     *     file paths
     */
    public static String resolveAnchored(Source source, Mapping mapping) {
        return resolveFilePathOrThrow(source, mapping).toString();
    }

    /**
     * Resolves the effective filesystem {@link Path} for a {@link FilePath} source. See
     * {@link #resolveAnchored(Source, Mapping)} for semantics and error conditions.
     */
    public static Path resolveAnchoredPath(Source source, Mapping mapping) {
        return resolveFilePathOrThrow(source, mapping);
    }

    /**
     * Resolves the parent directory of the (single) mapping file declared on {@code mapping}.
     * Generic helper available to any caller that already knows it needs the mapping directory —
     * notably reference-formulation-specific sources whose URL anchors against the mapping
     * directory by definition.
     *
     * @param source the source the directory is being resolved for; used only to enrich diagnostic
     *     messages
     * @param mapping the mapping context whose mapping file paths name the directory; must declare
     *     exactly one mapping file path
     * @return the parent directory of the mapping file, or {@code "/"} when the mapping file sits
     *     directly under the filesystem root
     * @throws SourceResolverException if {@code mapping} is {@code null}, declares no mapping file
     *     paths, or declares more than one mapping file path
     */
    public static Path resolveMappingDirectory(Source source, Mapping mapping) {
        if (mapping == null || mapping.getMappingFilePaths().isEmpty()) {
            throw new SourceResolverException(
                    "No mapping file paths provided for source %s.".formatted(diagnosticName(source)));
        }

        var mappingDirs =
                mapping.getMappingFilePaths().stream().map(Path::getParent).toList();

        if (mappingDirs.size() > 1) {
            throw new SourceResolverException(
                    "Multiple mapping directories found, where only one was expected, for source %s."
                            .formatted(diagnosticName(source)));
        }

        // mappingFilePaths is non-empty (checked above) and stream() preserves cardinality, so
        // mappingDirs always has exactly one entry here. A null parent indicates the mapping file
        // sits in the filesystem root; resolve against "/" so paths like "data.csv" become
        // "/data.csv".
        var mappingDir = mappingDirs.get(0);
        return mappingDir != null ? mappingDir : Paths.get("/");
    }

    private static Path resolveFilePathOrThrow(Source source, Mapping mapping) {
        if (source == null) {
            throw new IllegalArgumentException("LogicalSource has no source defined");
        }
        if (!(source instanceof FilePath filePath)) {
            throw new IllegalArgumentException(
                    "Unsupported source type for SourcePathResolver: %s. Only FilePath is handled by this resolver."
                            .formatted(source.getClass().getName()));
        }
        return resolveFilePath(filePath, mapping);
    }

    private static Path resolveFilePath(FilePath filePath, Mapping mapping) {
        var pathString = filePath.getPath();
        if (pathString == null || pathString.isBlank()) {
            throw new IllegalArgumentException("FilePath has no path defined");
        }

        var path = Path.of(pathString);
        var root = filePath.getRoot();

        if (path.isAbsolute()) {
            warnIfAbsoluteWithRoot(filePath, pathString, root);
            return path;
        }

        return resolveAnchor(filePath, root, mapping).resolve(path);
    }

    private static Path resolveAnchor(FilePath filePath, Value root, Mapping mapping) {
        if (root instanceof IRI iri && Rml.MappingDirectory.equals(iri)) {
            return resolveMappingDirectory(filePath, mapping);
        }
        // rml:CurrentWorkingDirectory, no rml:root, or any unrecognized root IRI all default to
        // the JVM working directory.
        return Paths.get("");
    }

    private static void warnIfAbsoluteWithRoot(FilePath filePath, String pathString, Value root) {
        if (root == null) {
            return;
        }

        var dedupKey = dedupKeyFor(filePath);
        if (!WARNED_ABSOLUTE_WITH_ROOT.add(dedupKey)) {
            return;
        }

        LOG.warn(
                "FilePath source <{}> declares an absolute rml:path \"{}\" together with rml:root <{}>;"
                        + " rml:root is unused for absolute paths and will be ignored."
                        + " Either remove rml:root or change rml:path to a relative value.",
                filePath.getResourceName(),
                pathString,
                root);
    }

    private static String dedupKeyFor(FilePath filePath) {
        var resourceName = filePath.getResourceName();
        if (resourceName != null && !resourceName.isBlank()) {
            return resourceName;
        }
        // Blank-node sources without a stable IRI / label fall back to identity so each occurrence
        // still dedups within a JVM process while different blank-node sources warn independently.
        return "@bnode-%d".formatted(System.identityHashCode(filePath));
    }

    /**
     * Returns a short identifier for {@code source} suitable for use in diagnostic messages.
     * Falls back gracefully when {@link io.carml.util.LogUtil#exception(Resource)}
     * would NPE (e.g. on test mocks that leave {@code asRdf()} unstubbed): in that case the
     * source's class name plus identity hash is returned, ensuring exception messages stay readable
     * even when the richer RDF-graph diagnostic is unavailable.
     */
    private static String diagnosticName(Source source) {
        if (source == null) {
            return "<null>";
        }
        try {
            return exception(source);
        } catch (NullPointerException npe) {
            return "%s@%d".formatted(source.getClass().getSimpleName(), System.identityHashCode(source));
        }
    }
}
