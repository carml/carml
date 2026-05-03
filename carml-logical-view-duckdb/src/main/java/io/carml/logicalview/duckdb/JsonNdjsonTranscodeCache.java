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
 * targets a top-level array, a top-level object's property values (when iterator is {@code $[*]}
 * or {@code $.*} over a {@code {}}-rooted file), or a named sub-array (no recursive descent, no
 * slice/union selectors). The transcode listener additionally aborts when any emitted record is
 * not a JSON object, since DuckDB's {@code read_ndjson_objects} requires object-shaped lines —
 * arrays of scalars and object maps with mixed values therefore fall back to {@code read_text}.
 * Anything outside that envelope preserves exact behavior for the small-file conformance suite.
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

    /**
     * Byte budget for the structural-byte peek used by {@link #peekFirstStructuralByte} to decide
     * whether the file root is {@code [} or {@code {}}. Distinct from {@link #NDJSON_PEEK_BYTES},
     * which is the wider budget used by {@link #isNdjsonShape} (and the helpers it dispatches to)
     * when looking for two consecutive top-level objects.
     */
    private static final int PEEK_BYTES = 32;

    /**
     * Byte budget for the NDJSON-shape peek. Wide enough to fit several typical records back-to-back
     * but small enough that the I/O is negligible. If the first record exceeds this budget the peek
     * conservatively returns "not NDJSON" and the caller falls back; the optimization is missed but
     * correctness is preserved.
     */
    private static final int NDJSON_PEEK_BYTES = 4096;

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
     * <p>See {@link #tryGetDirectNdjsonSourceSql} for the sibling check that handles files which
     * are <em>already</em> NDJSON-shaped — that branch should run before this one so a real NDJSON
     * file is read directly via {@code read_ndjson_objects} rather than being misclassified as an
     * object root and JSurfer-streamed.
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
     * Returns the SQL fragment that reads the source as NDJSON directly via DuckDB's
     * {@code read_ndjson_objects} when the file is genuinely NDJSON-shaped, or
     * {@link Optional#empty()} otherwise. Unlike {@link #tryGetSourceSql} this is a static check
     * with no transcoding or caching: the source file IS the NDJSON, so we just point DuckDB at it.
     *
     * <p>Eligibility:
     * <ul>
     *   <li>{@code filePath} resolves to a local regular file (no remote URLs).</li>
     *   <li>{@code basePath} (after analyzer normalization) is {@code $} or {@code $[*]} — anything
     *       deeper assumes structure inside a single root document and is incompatible with the
     *       record-per-line NDJSON shape.</li>
     *   <li>No slice or union selectors in the iterator (those would constrain the rows after
     *       reading; the read itself is full-file).</li>
     *   <li>A {@value #NDJSON_PEEK_BYTES}-byte file peek confirms NDJSON shape: first non-whitespace
     *       byte is {@code '{'}, the first complete top-level object closes, and another {@code '{'}
     *       follows after intervening whitespace. A single-object file or a file whose first record
     *       exceeds the peek budget conservatively falls back to {@link Optional#empty()}.</li>
     * </ul>
     *
     * <p>This branch must run BEFORE {@link #tryGetSourceSql} on the caller's side: a {@code {}}-rooted
     * NDJSON file would otherwise look "object-rooted" to {@link #toStreamingPath} and the transcode
     * would try to JSurfer-stream it as a single root object, producing wrong output.
     */
    static Optional<String> tryGetDirectNdjsonSourceSql(
            String filePath, String basePath, boolean hasSlice, boolean hasUnion) {
        if (hasSlice || hasUnion) {
            return Optional.empty();
        }
        if (filePath == null || filePath.isBlank() || basePath == null || basePath.isBlank()) {
            return Optional.empty();
        }
        if (filePath.contains("://")) {
            return Optional.empty();
        }
        var normalized = JsonPathSourceHandler.normalizeChildWildcard(basePath.strip());
        if (!"$".equals(normalized) && !"$[*]".equals(normalized)) {
            return Optional.empty();
        }
        var path = Path.of(filePath);
        if (!Files.isRegularFile(path)) {
            return Optional.empty();
        }
        if (!isNdjsonShape(path)) {
            return Optional.empty();
        }
        return Optional.of(buildSourceSql(path));
    }

    /**
     * Validates and resolves the cache key for the given file/base-path pair, or returns {@code null}
     * if transcoding is not applicable. Centralizing the eligibility checks here keeps
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
     * {@code null} when the path uses recursive descent or the file root does not match the
     * iterator shape (e.g. {@code $} over a non-array-rooted file). For {@code $[*]} the streaming
     * path adapts to the file root: {@code $[*]} for array-rooted files, {@code $.*} for
     * object-rooted files (so JSurfer iterates property values). Slice and union selectors are
     * filtered out by the caller (via the flags on {@link #tryGetSourceSql}), since the analyzer
     * rewrites them to a plain {@code [*]} walker by the time the path reaches this method.
     */
    private static String toStreamingPath(String basePath, Path file) {
        var normalized = JsonPathSourceHandler.normalizeChildWildcard(basePath.strip());
        if (normalized.contains("..")) {
            return null;
        }

        if ("$".equals(normalized)) {
            // Iterator $ only iterates multiple records when the document is a top-level array.
            // A top-level object yields a single record and gains nothing from transcoding.
            return peekFirstStructuralByte(file) == '[' ? "$[*]" : null;
        }
        if ("$[*]".equals(normalized)) {
            var first = peekFirstStructuralByte(file);
            if (first == '[') {
                return "$[*]";
            }
            if (first == '{') {
                // Iterator $[*] over a top-level object iterates property values; JSurfer expresses
                // that as $.* (the JSONPath child wildcard) rather than $[*].
                return "$.*";
            }
            return null;
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
        // We bail on anything more elaborate to keep transcode behavior predictable.
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
     * Peeks the file and returns the first non-whitespace byte, or {@code -1} when the file is
     * empty / unreadable. Skips a leading UTF-8 BOM ({@code 0xEF 0xBB 0xBF}) so BOM-prefixed JSON
     * files are not misclassified.
     */
    private static int peekFirstStructuralByte(Path file) {
        try (var in = new BufferedInputStream(Files.newInputStream(file))) {
            skipBom(in);
            for (var i = 0; i < PEEK_BYTES; i++) {
                var nextByte = in.read();
                if (nextByte < 0) {
                    return -1;
                }
                if (!Character.isWhitespace((char) nextByte)) {
                    return nextByte;
                }
            }
        } catch (IOException e) {
            LOG.debug("Could not peek file [{}] for structural-byte check", file, e);
        }
        return -1;
    }

    /**
     * Peeks the first {@value #NDJSON_PEEK_BYTES} bytes (after any UTF-8 BOM) and returns
     * {@code true} when the file shape is consistent with NDJSON: first non-whitespace byte is
     * {@code '{'}, the first complete top-level object closes within the budget, and another
     * {@code '{'} follows (after intervening whitespace). The scan runs in three phases — locate
     * the opening brace, consume the first object (with string-aware brace depth), confirm the
     * next non-whitespace byte is another opening brace — each phase isolated in a small helper
     * to keep cognitive complexity low.
     */
    private static boolean isNdjsonShape(Path file) {
        try (var in = new BufferedInputStream(Files.newInputStream(file))) {
            skipBom(in);
            var afterOpen = findFirstOpeningBrace(in, NDJSON_PEEK_BYTES);
            if (afterOpen < 0) {
                return false;
            }
            var afterClose = consumeFirstObject(in, afterOpen);
            if (afterClose < 0) {
                return false;
            }
            return nextNonWhitespaceIsOpeningBrace(in, afterClose);
        } catch (IOException e) {
            LOG.debug("Could not peek file [{}] for NDJSON shape", file, e);
            return false;
        }
    }

    /**
     * Skips a leading UTF-8 BOM ({@code 0xEF 0xBB 0xBF}) on the given mark-supporting stream so
     * the next byte read by callers is the first content byte. If no BOM is present the stream is
     * reset to its original position. Callers must not rely on {@code mark}/{@code reset} after
     * this method returns.
     */
    private static void skipBom(BufferedInputStream in) throws IOException {
        in.mark(UTF8_BOM.length);
        var bom = new byte[UTF8_BOM.length];
        var bomRead = in.readNBytes(bom, 0, UTF8_BOM.length);
        if (bomRead != UTF8_BOM.length || !java.util.Arrays.equals(bom, UTF8_BOM)) {
            in.reset();
        }
    }

    /**
     * Skips whitespace until the first non-whitespace byte. Returns the remaining peek budget
     * after consuming the {@code '{'} when the first non-whitespace byte is an opening brace,
     * otherwise {@code -1}.
     */
    private static int findFirstOpeningBrace(BufferedInputStream in, int budget) throws IOException {
        while (budget > 0) {
            var b = in.read();
            budget--;
            if (b < 0) {
                return -1;
            }
            if (!Character.isWhitespace((char) b)) {
                return b == '{' ? budget : -1;
            }
        }
        return -1;
    }

    /**
     * Walks bytes from inside the first object (one {@code '{'} already consumed) until its
     * matching {@code '}'} closes the depth-1 frame. Strings are skipped via {@link #consumeString}
     * so embedded {@code {} braces inside string values do not confuse the depth counter. Returns
     * the remaining peek budget on success, {@code -1} if the object did not close within the
     * budget or the file ended mid-object.
     */
    private static int consumeFirstObject(BufferedInputStream in, int budget) throws IOException {
        var depth = 1;
        while (budget > 0) {
            var b = in.read();
            budget--;
            if (b < 0) {
                return -1;
            }
            if (b == '"') {
                budget = consumeString(in, budget);
                if (budget < 0) {
                    return -1;
                }
            } else if (b == '{') {
                depth++;
            } else if (b == '}' && --depth == 0) {
                return budget;
            }
        }
        return -1;
    }

    /**
     * Consumes bytes from inside a JSON string (one opening {@code '"'} already consumed) until the
     * matching close quote. Skips the byte after a backslash so escaped quotes do not terminate the
     * string prematurely. Returns the remaining peek budget after consuming the closing {@code '"'},
     * or {@code -1} if the string did not close within the budget.
     */
    private static int consumeString(BufferedInputStream in, int budget) throws IOException {
        while (budget > 0) {
            var b = in.read();
            budget--;
            if (b < 0) {
                return -1;
            }
            if (b == '\\') {
                if (in.read() < 0) {
                    return -1;
                }
                budget--;
            } else if (b == '"') {
                return budget;
            }
        }
        return -1;
    }

    /**
     * Skips whitespace and returns {@code true} when the next non-whitespace byte within the
     * remaining budget is {@code '{'}, otherwise {@code false} (including when the file ends or
     * the budget is exhausted before a non-whitespace byte appears).
     */
    private static boolean nextNonWhitespaceIsOpeningBrace(BufferedInputStream in, int budget) throws IOException {
        while (budget > 0) {
            var b = in.read();
            budget--;
            if (b < 0) {
                return false;
            }
            if (!Character.isWhitespace((char) b)) {
                return b == '{';
            }
        }
        return false;
    }

    private Path nextOutputPath() {
        var name = "%s%s_%d.ndjson".formatted(TRANSCODE_PREFIX, instancePrefix, counter.getAndIncrement());
        return cacheDir.resolve(name);
    }

    private static boolean transcode(Path source, String jsurferPath, Path target) {
        if (!ensureCacheDirectory(target)) {
            return false;
        }
        try (var in = new BufferedInputStream(Files.newInputStream(source));
                var out = new BufferedOutputStream(Files.newOutputStream(target))) {
            var configBuilder = JSON_SURFER.configBuilder();
            configBuilder.bind(jsurferPath, (value, context) -> writeRecord(value, jsurferPath, out));
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
            deletePartialOutput(target);
            return false;
        }
    }

    private static boolean ensureCacheDirectory(Path target) {
        try {
            Files.createDirectories(target.getParent() == null ? target : target.getParent());
            return true;
        } catch (IOException e) {
            LOG.warn("Failed to ensure NDJSON cache directory exists for [{}]", target, e);
            return false;
        }
    }

    /**
     * Writes one transcoded record as a single NDJSON line. Aborts the surf via
     * {@link TranscodeAborted} when the value isn't a {@link JsonNode}, isn't an object (DuckDB's
     * {@code read_ndjson_objects} requires object-shaped lines), or the underlying write fails.
     */
    private static void writeRecord(Object value, String jsurferPath, BufferedOutputStream out) {
        if (!(value instanceof JsonNode node)) {
            throw new TranscodeAborted(new IOException("JSurfer produced non-JsonNode value: %s".formatted(value)));
        }
        if (!node.isObject()) {
            throw new TranscodeAborted(new IOException("Non-object record encountered while transcoding [%s]: %s"
                    .formatted(jsurferPath, node.getNodeType())));
        }
        try {
            out.write(OBJECT_MAPPER.writeValueAsBytes(node));
            out.write('\n');
        } catch (IOException ioe) {
            throw new TranscodeAborted(ioe);
        }
    }

    private static void deletePartialOutput(Path target) {
        try {
            Files.deleteIfExists(target);
        } catch (IOException e) {
            LOG.warn("Failed to delete partial NDJSON file [{}] after transcode error", target, e);
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
