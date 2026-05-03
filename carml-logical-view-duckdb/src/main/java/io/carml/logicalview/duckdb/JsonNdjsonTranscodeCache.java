package io.carml.logicalview.duckdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.Serial;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.jooq.impl.DSL;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.JsonSurferJackson;
import org.jsfr.json.SurfingConfiguration;

/**
 * Caches stream-transcoded NDJSON copies of large JSON-array source files so the DuckDB engine can
 * read them via {@code read_ndjson_objects}, which iterates element by element with bounded memory
 * usage. The default {@link JsonPathSourceHandler} fallback path uses {@code read_text} +
 * {@code json_extract} + {@code unnest}, which slurps the entire file into a single VARCHAR before
 * iterating. For files exceeding {@link #DEFAULT_SIZE_THRESHOLD_BYTES} this exhausts the DuckDB
 * memory limit on realistic mappings.
 *
 * <p>Eligibility for transcoding is intentionally narrow: the source must be a local regular file
 * larger than the threshold, and the iterator base path must be a stream-friendly walker that
 * targets a top-level or sub-array (no recursive descent, no slice/union selectors). Anything
 * outside that envelope falls back to the existing {@code read_text} compilation, preserving exact
 * behaviour for the small-file conformance suite.
 *
 * <p>Transcoded NDJSON files live under the cache directory configured at construction time (the
 * same {@code /carml-spill} or sibling-of-database location used by DuckDB's {@code temp_directory})
 * and are deleted when the cache is {@link #close() closed}. Failed transcode attempts delete
 * partial output and are not cached, so a transient I/O failure does not poison subsequent calls.
 *
 * <p><strong>Thread safety:</strong> The cache is thread-safe via per-key locking using
 * {@link ConcurrentHashMap#computeIfAbsent}. Two evaluator threads requesting the same
 * {@link CacheKey} concurrently share a single transcode operation; threads requesting different
 * keys proceed in parallel.
 */
@Slf4j
final class JsonNdjsonTranscodeCache implements AutoCloseable {

    /** Default size above which JSON-array files are eligible for NDJSON transcoding (50 MB). */
    static final long DEFAULT_SIZE_THRESHOLD_BYTES = 50L * 1024 * 1024;

    private static final String TRANSCODE_PREFIX = "__carml_ndjson_";

    private static final int PEEK_BYTES = 32;

    private static final byte[] UTF8_BOM = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final JsonSurfer JSON_SURFER = JsonSurferJackson.INSTANCE;

    private final Path cacheDir;
    private final long sizeThresholdBytes;
    /** Per-key locking via {@link ConcurrentHashMap#computeIfAbsent}. */
    private final ConcurrentHashMap<CacheKey, Path> cache = new ConcurrentHashMap<>();

    private final AtomicInteger counter = new AtomicInteger(0);
    private final String instancePrefix;

    /**
     * Creates a cache rooted at the given directory using the {@link #DEFAULT_SIZE_THRESHOLD_BYTES}
     * default size threshold.
     *
     * @param cacheDir directory under which transcoded NDJSON files are stored; must exist or be
     *     creatable by the caller
     */
    JsonNdjsonTranscodeCache(Path cacheDir) {
        this(cacheDir, DEFAULT_SIZE_THRESHOLD_BYTES);
    }

    /**
     * Creates a cache with an explicit size threshold. Tests use this constructor to drive
     * transcoding with small fixtures.
     *
     * @param cacheDir directory under which transcoded NDJSON files are stored
     * @param sizeThresholdBytes file-size threshold above which transcoding kicks in; must be
     *     non-negative
     */
    JsonNdjsonTranscodeCache(Path cacheDir, long sizeThresholdBytes) {
        if (cacheDir == null) {
            throw new IllegalArgumentException("cacheDir must not be null");
        }
        if (sizeThresholdBytes < 0) {
            throw new IllegalArgumentException("sizeThresholdBytes must be non-negative");
        }
        this.cacheDir = cacheDir;
        this.sizeThresholdBytes = sizeThresholdBytes;
        this.instancePrefix = UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Returns the SQL fragment to use as the source clause when transcoding is applicable, or
     * {@link Optional#empty()} otherwise. Reasons for declining include: the file is not a local
     * regular file (e.g., a remote URL), the file is at or below the size threshold, the base path
     * uses recursive descent, slice or union selectors, or otherwise cannot be expressed as a
     * JSurfer streaming walker, the file's structure does not match the expected shape (top-level
     * non-array for {@code $}-rooted iterators), or the transcode operation failed.
     *
     * <p>Slice and union selectors are detected via the {@code hasSlice}/{@code hasUnion} flags
     * because the upstream {@link JsonPathAnalyzer} normalizes the {@code basePath} to a plain
     * {@code [*]} walker and stashes those selectors separately. Filter expressions are handled
     * upstream as a SQL {@code WHERE} wrapper around whatever source SQL this cache produces, so
     * filters do not block transcoding.
     *
     * <p>On success the result has the shape
     * {@code (SELECT json AS "__iter" FROM read_ndjson_objects('<absolute_ndjson_path>'))} so the
     * downstream {@link JsonIteratorSourceStrategy} can keep using the {@code __iter} column
     * unchanged.
     *
     * @param filePath absolute path of the source JSON file
     * @param basePath the analyzer-normalized streaming base path (e.g. {@code $.records[*]})
     * @param hasSlice {@code true} if the iterator includes a slice selector ({@code [s:e:step]})
     * @param hasUnion {@code true} if the iterator includes a union selector ({@code [0,2,5]} or
     *     {@code ['a','b']})
     */
    Optional<String> tryGetSourceSql(String filePath, String basePath, boolean hasSlice, boolean hasUnion) {
        if (hasSlice || hasUnion) {
            return Optional.empty();
        }
        var key = buildCacheKey(filePath, basePath);
        if (key == null) {
            return Optional.empty();
        }

        var resolved = cache.computeIfAbsent(key, k -> {
            var out = nextOutputPath();
            if (!transcode(Path.of(k.filePath()), k.basePath(), out)) {
                // ConcurrentHashMap.computeIfAbsent does not record a mapping when the function
                // returns null and atomically releases the per-key lock — so a failed transcode
                // does not poison the cache for subsequent attempts.
                return null;
            }
            LOG.debug("Transcoded JSON [{}] @ [{}] -> NDJSON [{}]", k.filePath(), k.basePath(), out);
            return out;
        });
        if (resolved == null) {
            return Optional.empty();
        }
        return Optional.of(buildSourceSql(resolved));
    }

    /**
     * Validates and resolves the cache key for the given file/base-path pair, or returns {@code null}
     * if transcoding is not applicable. Centralising the eligibility checks here keeps
     * {@link #tryGetSourceSql} readable and below the cyclomatic complexity budget.
     */
    private CacheKey buildCacheKey(String filePath, String basePath) {
        if (filePath == null || filePath.isBlank() || basePath == null || basePath.isBlank()) {
            return null;
        }
        if (filePath.contains("://")) {
            // Remote URLs (http, s3, etc.) cannot be size-checked locally — leave the heavy lifting
            // to DuckDB's read_text fallback.
            return null;
        }
        var path = Path.of(filePath);
        if (!Files.isRegularFile(path)) {
            return null;
        }
        long size;
        long mtime;
        try {
            size = Files.size(path);
            mtime = Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            LOG.debug("Could not stat file [{}] for NDJSON transcode eligibility", filePath, e);
            return null;
        }
        if (size <= sizeThresholdBytes) {
            return null;
        }
        var jsurferPath = toStreamingPath(basePath, path);
        if (jsurferPath == null) {
            return null;
        }
        return new CacheKey(path.toAbsolutePath().toString(), mtime, size, jsurferPath);
    }

    @Override
    public void close() {
        for (var path : cache.values()) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                LOG.warn("Failed to delete transcoded NDJSON file [{}]", path, e);
            }
        }
        cache.clear();
    }

    /** Returns the number of cached transcoded NDJSON files. */
    int size() {
        return cache.size();
    }

    /**
     * Converts the DuckDB iterator base path into a JSurfer-compatible streaming path. Returns
     * {@code null} when the path uses recursive descent or is {@code $} over a non-array-rooted
     * file. Slice and union selectors are filtered out by the caller (via the flags on
     * {@link #tryGetSourceSql}), since the analyzer rewrites them to a plain {@code [*]} walker
     * by the time the path reaches this method.
     */
    private static String toStreamingPath(String basePath, Path file) {
        var normalized = JsonPathSourceHandler.normalizeChildWildcard(basePath.strip());
        if (normalized.contains("..")) {
            return null;
        }

        if ("$".equals(normalized) || "$[*]".equals(normalized)) {
            return isArrayRooted(file) ? "$[*]" : null;
        }

        // Eligible walker: $.<segment>(.<segment>)*[*]
        if (!normalized.endsWith("[*]") || !normalized.startsWith("$.")) {
            return null;
        }

        var inner = normalized.substring(2, normalized.length() - 3);
        if (inner.isEmpty()) {
            return null;
        }
        // Each segment must be a plain identifier (no further [*] in the middle, no other selectors).
        // We bail on anything more elaborate to keep transcode behaviour predictable.
        var segmentsValid =
                java.util.Arrays.stream(inner.split("\\.", -1)).allMatch(JsonNdjsonTranscodeCache::isPlainSegment);
        return segmentsValid ? normalized : null;
    }

    private static boolean isPlainSegment(String segment) {
        if (segment.isEmpty()) {
            return false;
        }
        return segment.chars().allMatch(c -> c == '_' || Character.isLetterOrDigit(c));
    }

    /**
     * Peeks the file to confirm the first non-whitespace byte is {@code [}. Skips a leading UTF-8
     * BOM ({@code 0xEF 0xBB 0xBF}) before scanning so BOM-prefixed JSON files are not misclassified
     * as object-rooted.
     */
    private static boolean isArrayRooted(Path file) {
        try (var in = new BufferedInputStream(Files.newInputStream(file))) {
            in.mark(UTF8_BOM.length);
            var bom = new byte[UTF8_BOM.length];
            var bomRead = in.readNBytes(bom, 0, UTF8_BOM.length);
            if (bomRead != UTF8_BOM.length || !java.util.Arrays.equals(bom, UTF8_BOM)) {
                in.reset();
            }

            for (var i = 0; i < PEEK_BYTES; i++) {
                var nextByte = in.read();
                if (nextByte < 0) {
                    return false;
                }
                if (Character.isWhitespace((char) nextByte)) {
                    continue;
                }
                return nextByte == '[';
            }
        } catch (IOException e) {
            LOG.debug("Could not peek file [{}] for array-rooted check", file, e);
        }
        return false;
    }

    private Path nextOutputPath() {
        var name = "%s%s_%d.ndjson".formatted(TRANSCODE_PREFIX, instancePrefix, counter.getAndIncrement());
        return cacheDir.resolve(name);
    }

    private static boolean transcode(Path source, String jsurferPath, Path target) {
        try {
            Files.createDirectories(target.getParent() == null ? target : target.getParent());
        } catch (IOException e) {
            LOG.warn("Failed to ensure NDJSON cache directory exists for [{}]", target, e);
            return false;
        }

        try (var in = new BufferedInputStream(Files.newInputStream(source));
                var out = new BufferedOutputStream(Files.newOutputStream(target))) {
            var configBuilder = JSON_SURFER.configBuilder();
            configBuilder.bind(jsurferPath, (value, context) -> {
                if (!(value instanceof JsonNode node)) {
                    throw new TranscodeAborted(
                            new IOException("JSurfer produced non-JsonNode value: %s".formatted(value)));
                }
                try {
                    out.write(OBJECT_MAPPER.writeValueAsBytes(node));
                    out.write('\n');
                } catch (IOException ioe) {
                    throw new TranscodeAborted(ioe);
                }
            });
            surf(in, configBuilder.build());
            return true;
        } catch (IOException | RuntimeException e) {
            LOG.warn(
                    "Failed to transcode JSON source [{}] @ [{}] to NDJSON [{}]: {}",
                    source,
                    jsurferPath,
                    target,
                    e.getMessage(),
                    e);
            try {
                Files.deleteIfExists(target);
            } catch (IOException ioe) {
                LOG.warn("Failed to delete partial NDJSON file [{}] after transcode error", target, ioe);
            }
            return false;
        }
    }

    /**
     * Runs JSurfer over the input stream, translating a thrown {@link TranscodeAborted} sentinel
     * back into its underlying {@link IOException}. Extracted so the surrounding try-with-resources
     * holds a single try block.
     */
    private static void surf(BufferedInputStream in, SurfingConfiguration config) throws IOException {
        try {
            JSON_SURFER.surf(in, config);
        } catch (TranscodeAborted aborted) {
            throw aborted.getCause();
        }
    }

    private static String buildSourceSql(Path ndjsonPath) {
        return "(SELECT json AS \"__iter\" FROM read_ndjson_objects(%s))"
                .formatted(DSL.inline(ndjsonPath.toAbsolutePath().toString()));
    }

    /**
     * Cache key combining the source file's identity (absolute path), its modification time, its
     * size and the streaming path used for transcoding. mtime + size invalidate stale entries when
     * the source file is rewritten between calls; the streaming path differentiates entries when
     * different iterators target the same file.
     */
    record CacheKey(String filePath, long mtime, long size, String basePath) {}

    /**
     * Sentinel thrown out of the JSurfer listener to abort the surf early when an unrecoverable
     * problem is detected (non-{@link JsonNode} value or downstream {@link IOException}). JSurfer
     * does not expose a direct cancellation hook, so we bail by throwing.
     */
    private static final class TranscodeAborted extends RuntimeException {

        @Serial
        private static final long serialVersionUID = 1L;

        TranscodeAborted(IOException cause) {
            super(cause);
        }

        @Override
        public synchronized IOException getCause() {
            return (IOException) super.getCause();
        }
    }
}
