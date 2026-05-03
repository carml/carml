package io.carml.logicalview.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JsonNdjsonTranscodeCacheTest {

    /** Low threshold so the test fixtures qualify without writing tens of megabytes per test. */
    private static final long TEST_THRESHOLD = 1024L;

    @TempDir
    Path tempDir;

    private Path cacheDir;

    private JsonNdjsonTranscodeCache cache;

    @BeforeEach
    void beforeEach() throws IOException {
        cacheDir = Files.createDirectory(tempDir.resolve("cache"));
        cache = new JsonNdjsonTranscodeCache(cacheDir, TEST_THRESHOLD);
    }

    @AfterEach
    void afterEach() {
        cache.close();
    }

    @Test
    void tryGetSourceSql_smallFile_returnsEmpty() throws IOException {
        var sourceFile = tempDir.resolve("small.json");
        Files.writeString(sourceFile, "[{\"id\":1}]");

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$[*]", false, false);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void tryGetSourceSql_largeArrayRoot_transcodesAndReturnsSql() throws IOException {
        var sourceFile = tempDir.resolve("array.json");
        var recordCount = writeJsonArrayFile(sourceFile, 200, "value");

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$[*]", false, false);

        assertThat(result.isPresent(), is(true));
        var sql = result.orElseThrow();
        assertThat(sql, containsString("read_ndjson_objects"));
        assertThat(sql, containsString("SELECT json AS \"__iter\""));
        var ndjsonPath = onlyNdjsonFileInCacheDir();
        assertThat(sql, containsString(ndjsonPath.toAbsolutePath().toString()));
        assertThat(countLines(ndjsonPath), is(recordCount));
    }

    @Test
    void tryGetSourceSql_largeNestedArray_transcodesCorrectly() throws IOException {
        var sourceFile = tempDir.resolve("nested.json");
        var recordCount = writeNestedRecordsFile(sourceFile, 200, "value");

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$.records[*]", false, false);

        assertThat(result.isPresent(), is(true));
        var ndjsonPath = onlyNdjsonFileInCacheDir();
        assertThat(countLines(ndjsonPath), is(recordCount));
    }

    @Test
    void tryGetSourceSql_topLevelDollarOnObjectFile_returnsEmpty() throws IOException {
        // Pad with a long array of strings to push the file size above the threshold.
        var padding = "x".repeat((int) TEST_THRESHOLD * 2);
        var sourceFile = tempDir.resolve("object.json");
        Files.writeString(sourceFile, "{\"a\":1,\"pad\":\"%s\"}".formatted(padding));

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$", false, false);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void tryGetSourceSql_recursiveDescent_returnsEmpty() throws IOException {
        var sourceFile = tempDir.resolve("array.json");
        writeJsonArrayFile(sourceFile, 200, "value");

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$..foo", false, false);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void tryGetSourceSql_remoteUrl_returnsEmpty() {
        var result = cache.tryGetSourceSql("http://example.com/data.json", "$[*]", false, false);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void tryGetSourceSql_sameKey_returnsCachedPath() throws IOException {
        var sourceFile = tempDir.resolve("array.json");
        writeJsonArrayFile(sourceFile, 200, "value");

        var first = cache.tryGetSourceSql(sourceFile.toString(), "$[*]", false, false);
        var second = cache.tryGetSourceSql(sourceFile.toString(), "$[*]", false, false);

        assertThat(first.orElseThrow(), is(second.orElseThrow()));
        assertThat(listNdjsonFiles(), hasSize(1));
        assertThat(cache.size(), is(1));
    }

    @Test
    void tryGetSourceSql_modifiedFile_returnsNewPath() throws IOException {
        var sourceFile = tempDir.resolve("array.json");
        writeJsonArrayFile(sourceFile, 200, "first");
        var first = cache.tryGetSourceSql(sourceFile.toString(), "$[*]", false, false)
                .orElseThrow();

        // Rewrite the file with different contents and bump mtime so the cache key changes.
        writeJsonArrayFile(sourceFile, 250, "second");
        Files.setLastModifiedTime(sourceFile, FileTime.fromMillis(System.currentTimeMillis() + 5_000));

        var second = cache.tryGetSourceSql(sourceFile.toString(), "$[*]", false, false)
                .orElseThrow();

        assertThat(first, is(not(second)));
        // Both files should still be present until close()
        assertThat(listNdjsonFiles(), hasSize(2));
        assertThat(cache.size(), is(2));
    }

    @Test
    void tryGetSourceSql_invalidJson_returnsEmptyAndCleansUpPartialNdjson() throws IOException {
        var sourceFile = tempDir.resolve("malformed.json");
        var padding = "x".repeat((int) TEST_THRESHOLD * 2);
        Files.writeString(sourceFile, "[{\"id\":1, \"oops\" %s".formatted(padding));

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$[*]", false, false);

        assertThat(result.isPresent(), is(false));
        assertThat(listNdjsonFiles(), is(empty()));
    }

    @Test
    void close_deletesAllCachedNdjsonFiles() throws IOException {
        var sourceA = tempDir.resolve("a.json");
        writeJsonArrayFile(sourceA, 200, "value");
        var sourceB = tempDir.resolve("b.json");
        writeNestedRecordsFile(sourceB, 200, "value");

        cache.tryGetSourceSql(sourceA.toString(), "$[*]", false, false);
        cache.tryGetSourceSql(sourceB.toString(), "$.records[*]", false, false);
        assertThat(listNdjsonFiles(), hasSize(2));

        cache.close();

        assertThat(listNdjsonFiles(), is(empty()));
        assertThat(cache.size(), is(0));
    }

    @Test
    void tryGetSourceSql_blankInputs_returnEmpty() {
        assertThat(cache.tryGetSourceSql(null, "$[*]", false, false).isPresent(), is(false));
        assertThat(cache.tryGetSourceSql("", "$[*]", false, false).isPresent(), is(false));
        assertThat(cache.tryGetSourceSql("/tmp/x.json", null, false, false).isPresent(), is(false));
        assertThat(cache.tryGetSourceSql("/tmp/x.json", "  ", false, false).isPresent(), is(false));
    }

    @Test
    void tryGetSourceSql_nonexistentFile_returnsEmpty() {
        var result = cache.tryGetSourceSql(tempDir.resolve("missing.json").toString(), "$[*]", false, false);

        assertThat(result.isPresent(), is(false));
    }

    @Test
    void tryGetSourceSql_dollarBasePath_arrayRootedFile_transcodes() throws IOException {
        var sourceFile = tempDir.resolve("array.json");
        var recordCount = writeJsonArrayFile(sourceFile, 200, "value");

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$", false, false);

        assertThat(result.isPresent(), is(true));
        var ndjsonPath = onlyNdjsonFileInCacheDir();
        assertThat(countLines(ndjsonPath), is(recordCount));
    }

    @Test
    void tryGetSourceSql_pathWithSliceSelector_returnsEmpty() throws IOException {
        // The analyzer rewrites $.records[0:10] to basePath $.records[*] and stashes the slice
        // separately. Simulate that here by passing the [*]-rewritten basePath plus hasSlice=true.
        var sourceFile = tempDir.resolve("array.json");
        writeJsonArrayFile(sourceFile, 200, "value");

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$.records[*]", true, false);

        assertThat(result.isPresent(), is(false));
        assertThat(listNdjsonFiles(), is(empty()));
    }

    @Test
    void tryGetSourceSql_pathWithUnionSelector_returnsEmpty() throws IOException {
        // The analyzer rewrites $['a','b'] (or $.obj[0,2,5]) to basePath ending in [*] and
        // stashes the union separately. Simulate that here by passing the [*]-rewritten basePath
        // plus hasUnion=true.
        var sourceFile = tempDir.resolve("array.json");
        writeJsonArrayFile(sourceFile, 200, "value");

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$.obj[*]", false, true);

        assertThat(result.isPresent(), is(false));
        assertThat(listNdjsonFiles(), is(empty()));
    }

    @Test
    void tryGetSourceSql_arrayWithUtf8Bom_transcodes() throws IOException {
        var sourceFile = tempDir.resolve("bom.json");
        var recordCount = writeJsonArrayFileWithBom(sourceFile, 200, "value");

        var result = cache.tryGetSourceSql(sourceFile.toString(), "$[*]", false, false);

        assertThat(result.isPresent(), is(true));
        var ndjsonPath = onlyNdjsonFileInCacheDir();
        assertThat(countLines(ndjsonPath), is(recordCount));
    }

    @Test
    void constructor_nullCacheDir_throws() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (var cacheUnderTest = new JsonNdjsonTranscodeCache(null)) {
                fail("Expected IllegalArgumentException but constructor returned: " + cacheUnderTest);
            }
        });
    }

    // --- helpers ---

    /**
     * Writes a JSON array of {@code recordCount} objects to {@code path}, padded so the file size
     * exceeds {@link #TEST_THRESHOLD}. Returns the number of records written.
     */
    private int writeJsonArrayFile(Path path, int recordCount, String prefix) throws IOException {
        var padding = "x".repeat(50);
        try (var out = Files.newOutputStream(path)) {
            out.write('[');
            for (var i = 0; i < recordCount; i++) {
                if (i > 0) {
                    out.write(',');
                }
                writeRecord(out, i, prefix, padding);
            }
            out.write(']');
        }
        ensureAboveThreshold(path);
        return recordCount;
    }

    /**
     * Writes a JSON array file prefixed by the UTF-8 BOM ({@code 0xEF 0xBB 0xBF}). The cache must
     * skip the BOM when peeking for an array-rooted file or it would misclassify the document.
     */
    private int writeJsonArrayFileWithBom(Path path, int recordCount, String prefix) throws IOException {
        var padding = "x".repeat(50);
        try (var out = Files.newOutputStream(path)) {
            out.write(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF});
            out.write('[');
            for (var i = 0; i < recordCount; i++) {
                if (i > 0) {
                    out.write(',');
                }
                writeRecord(out, i, prefix, padding);
            }
            out.write(']');
        }
        ensureAboveThreshold(path);
        return recordCount;
    }

    /**
     * Writes a JSON document {@code {"records": [...]}} containing {@code recordCount} objects so
     * the path {@code $.records[*]} can stream them. Returns the number of records written.
     */
    private int writeNestedRecordsFile(Path path, int recordCount, String prefix) throws IOException {
        var padding = "x".repeat(50);
        try (var out = Files.newOutputStream(path)) {
            out.write("{\"records\":[".getBytes(StandardCharsets.UTF_8));
            for (var i = 0; i < recordCount; i++) {
                if (i > 0) {
                    out.write(',');
                }
                writeRecord(out, i, prefix, padding);
            }
            out.write("]}".getBytes(StandardCharsets.UTF_8));
        }
        ensureAboveThreshold(path);
        return recordCount;
    }

    private static void writeRecord(OutputStream out, int id, String prefix, String padding) throws IOException {
        var rec = "{\"id\":%d,\"name\":\"%s_%d\",\"pad\":\"%s\"}".formatted(id, prefix, id, padding);
        out.write(rec.getBytes(StandardCharsets.UTF_8));
    }

    private void ensureAboveThreshold(Path path) throws IOException {
        if (Files.size(path) <= TEST_THRESHOLD) {
            throw new IllegalStateException("Test fixture [%s] is %d bytes; expected > %d. Increase recordCount."
                    .formatted(path, Files.size(path), TEST_THRESHOLD));
        }
    }

    private List<Path> listNdjsonFiles() throws IOException {
        try (Stream<Path> entries = Files.list(cacheDir)) {
            return entries.filter(p -> p.getFileName().toString().endsWith(".ndjson"))
                    .toList();
        }
    }

    private Path onlyNdjsonFileInCacheDir() throws IOException {
        var files = listNdjsonFiles();
        assertThat(files, allOf(hasSize(1), not(nullValue())));
        return files.get(0);
    }

    private static int countLines(Path path) throws IOException {
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            return (int) lines.count();
        }
    }
}
