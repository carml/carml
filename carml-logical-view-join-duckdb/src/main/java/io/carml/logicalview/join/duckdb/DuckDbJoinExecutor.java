package io.carml.logicalview.join.duckdb;

import io.carml.logicalsourceresolver.PausableFluxBridge;
import io.carml.logicalview.EvaluatedValues;
import io.carml.logicalview.ExpressionMapEvaluator;
import io.carml.logicalview.JoinExecutor;
import io.carml.logicalview.JoinKeyExtractor;
import io.carml.logicalview.MatchedRow;
import io.carml.logicalview.ViewIteration;
import io.carml.model.Join;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Hybrid in-memory + DuckDB-backed {@link JoinExecutor}. Drains parents into a rolling buffer; if
 * the buffer count exceeds the configured spill threshold, opens a DuckDB connection (in-memory or
 * file-backed), appends the buffered parents into a temp table, and continues draining the
 * remainder of the parent stream straight into the temp table. Children are then drained into a
 * sibling temp table via Appender, a single SQL HASH JOIN runs, and {@link MatchedRow}s stream
 * back in child stream order.
 *
 * <p>When the parent count stays at or below the threshold, the executor uses the same HashMap
 * probe path as {@link io.carml.logicalview.InMemoryJoinExecutor} — no DuckDB connection is
 * opened, no perf regression for small joins.
 *
 * <p>The DuckDB SQL hash join is fully blocking: both sides of the join are loaded before the
 * query runs. This guarantees no missed matches. Memory bound is set by DuckDB's buffer manager;
 * intermediate spill is configurable via {@code temp_directory} (set to {@code workingDir} in
 * file-backed mode). The result stream is back-pressure aware via {@link PausableFluxBridge}.
 *
 * <p>Row payloads are encoded as Arrow IPC byte streams (see {@link ArrowBlobCodec}) into BLOB
 * columns. The codec stores all values as Utf8 with a parallel {@code naturalDatatypes} IRI map —
 * RDF-output-equivalent to the original {@code Map<String, Object>} because downstream lexical
 * form generation is datatype-driven, not Java-runtime-type-driven. {@code sourceEvaluation} is
 * always {@code null} on the join path and is dropped on the wire.
 *
 * <p>Join result rows are read back via DuckDB's Arrow C Data Interface (see
 * {@link ArrowJoinResultReader}), which transfers columnar batches with zero-copy from native
 * memory — avoiding the per-cell JNI overhead of JDBC {@code ResultSet.getObject()} /
 * {@code getArray()}.
 */
@Slf4j
public final class DuckDbJoinExecutor implements JoinExecutor {

    private static final String PARENT_TABLE = "parents";

    private static final String CHILD_TABLE = "children";

    private final int spillThreshold;

    private final boolean fileBacked;

    private final Path spillDir;

    private final ExpressionMapEvaluator evaluator;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    private Connection connection;

    private Path dbFile;

    private Path workingDir;

    private DuckDBAppender parentAppender;

    private int parentRowId;

    DuckDbJoinExecutor(int spillThreshold, boolean fileBacked, Path spillDir, ExpressionMapEvaluator evaluator) {
        this.spillThreshold = spillThreshold;
        this.fileBacked = fileBacked;
        this.spillDir = spillDir;
        this.evaluator = evaluator;
    }

    @Override
    public Flux<MatchedRow> matches(
            Flux<ViewIteration> parents,
            Flux<EvaluatedValues> children,
            List<Join> conditions,
            Set<String> parentReferenceableKeys,
            boolean leftJoin) {

        var keyArity = conditions.size();
        return Flux.defer(() -> {
            if (!subscribed.compareAndSet(false, true)) {
                return Flux.error(doubleSubscriptionError());
            }
            var buffer = new ArrayList<ViewIteration>();
            var spilled = new AtomicBoolean(false);
            return parents.doOnNext(parent ->
                            drainParent(parent, buffer, spilled, conditions, parentReferenceableKeys, keyArity))
                    .then(Mono.defer(() -> selectMatchFlux(
                            spilled, buffer, children, conditions, parentReferenceableKeys, leftJoin, keyArity)))
                    .flatMapMany(f -> f);
        });
    }

    private static IllegalStateException doubleSubscriptionError() {
        return new IllegalStateException(
                "DuckDbJoinExecutor.matches() must be called at most once per instance — the executor"
                        + " holds per-join state (parent row counter, DuckDB connection) that cannot be"
                        + " safely reused. Obtain a fresh instance via DuckDbJoinExecutorFactory.create().");
    }

    private void drainParent(
            ViewIteration parent,
            List<ViewIteration> buffer,
            AtomicBoolean spilled,
            List<Join> conditions,
            Set<String> parentReferenceableKeys,
            int keyArity) {
        if (spilled.get()) {
            appendParent(parent, conditions, parentReferenceableKeys, keyArity);
            return;
        }
        buffer.add(parent);
        if (buffer.size() > spillThreshold) {
            spillBufferedParents(buffer, conditions, parentReferenceableKeys, keyArity);
            spilled.set(true);
        }
    }

    private void spillBufferedParents(
            List<ViewIteration> buffer, List<Join> conditions, Set<String> parentReferenceableKeys, int keyArity) {
        openConnection();
        createTempTable(PARENT_TABLE, keyArity);
        parentAppender = createAppender(PARENT_TABLE);
        buffer.forEach(p -> appendParent(p, conditions, parentReferenceableKeys, keyArity));
        buffer.clear();
        LOG.debug("Spilling join parents to DuckDB after {} rows (threshold {})", parentRowId, spillThreshold);
    }

    private Mono<Flux<MatchedRow>> selectMatchFlux(
            AtomicBoolean spilled,
            List<ViewIteration> buffer,
            Flux<EvaluatedValues> children,
            List<Join> conditions,
            Set<String> parentReferenceableKeys,
            boolean leftJoin,
            int keyArity) {
        if (!spilled.get()) {
            return Mono.just(inMemoryFlux(buffer, children, conditions, parentReferenceableKeys, leftJoin));
        }
        closeParentAppender();
        return Mono.just(duckDbFlux(children, conditions, leftJoin, keyArity));
    }

    private void closeParentAppender() {
        try {
            parentAppender.close();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to close parent appender", e);
        }
        parentAppender = null;
    }

    private Flux<MatchedRow> inMemoryFlux(
            List<ViewIteration> parentList,
            Flux<EvaluatedValues> children,
            List<Join> conditions,
            Set<String> parentReferenceableKeys,
            boolean leftJoin) {
        var index = new HashMap<List<Object>, List<ViewIteration>>();
        for (var parent : parentList) {
            var key = JoinKeyExtractor.parentKey(conditions, parent, parentReferenceableKeys, evaluator);
            if (!key.isEmpty()) {
                index.computeIfAbsent(key, k -> new ArrayList<>()).add(parent);
            }
        }
        return children.flatMap(child -> {
            var ckey = JoinKeyExtractor.childKey(conditions, child, evaluator);
            var matched = ckey.isEmpty() ? List.<ViewIteration>of() : index.getOrDefault(ckey, List.of());
            if (matched.isEmpty() && !leftJoin) {
                return Mono.empty();
            }
            return Mono.just(new MatchedRow(child, matched));
        });
    }

    private Flux<MatchedRow> duckDbFlux(
            Flux<EvaluatedValues> children, List<Join> conditions, boolean leftJoin, int keyArity) {
        // Flux.using owns the child appender's lifecycle: created on subscribe, closed on
        // terminate / cancel / error. Children are drained synchronously into the appender via
        // doOnNext; the appender is then explicitly closed before executeJoinAndStream so the
        // subsequent SELECT sees committed rows. The Flux.using cleanup is a belt-and-suspenders
        // close that handles error paths where the explicit close didn't run.
        return Flux.using(
                () -> {
                    createTempTable(CHILD_TABLE, keyArity);
                    return createAppender(CHILD_TABLE);
                },
                childAppender -> {
                    var childRowId = new AtomicInteger();
                    return children.doOnNext(child -> {
                                var key = JoinKeyExtractor.childKey(conditions, child, evaluator);
                                appendRow(
                                        childAppender,
                                        childRowId.getAndIncrement(),
                                        key,
                                        ArrowBlobCodec.encodeChild(child),
                                        keyArity);
                            })
                            .then(Mono.fromRunnable(() -> closeChildAppender(childAppender)))
                            .thenMany(Flux.defer(() -> executeJoinAndStream(leftJoin, keyArity)));
                },
                this::closeChildAppender);
    }

    private void closeChildAppender(DuckDBAppender appender) {
        try {
            appender.close();
        } catch (SQLException e) {
            // close() is idempotent on the underlying DuckDB appender; an exception here on a
            // double-close is benign. Log at debug to surface real failures without noise.
            LOG.debug("Failed to close child appender: {}", e.getMessage());
        }
    }

    private Flux<MatchedRow> executeJoinAndStream(boolean leftJoin, int keyArity) {
        var sql = buildJoinSql(leftJoin, keyArity);
        // Capture the source so onDispose can cancel it: the producer thread parks via
        // DuckDbPausableSource.awaitDemand() and would otherwise stay parked forever if the
        // subscriber cancels (or errors) — leaking the thread and blocking close()'s connection
        // shutdown.
        var sourceRef = new AtomicReference<DuckDbPausableSource>();
        return PausableFluxBridge.<MatchedRow>builder()
                .sourceFactory(emitter -> {
                    var source = new DuckDbPausableSource(self -> streamJoinResults(sql, emitter, self));
                    sourceRef.set(source);
                    return source;
                })
                .onDispose(() -> {
                    var source = sourceRef.get();
                    if (source != null) {
                        source.cancel();
                    }
                })
                .flux();
    }

    private void streamJoinResults(
            String sql, PausableFluxBridge.Emitter<MatchedRow> emitter, DuckDbPausableSource source) {
        try (var stmt = connection.createStatement();
                var rresultSet = stmt.executeQuery(sql)) {
            if (!(rresultSet instanceof DuckDBResultSet duckDbResultSet)) {
                throw new IllegalStateException("Expected DuckDBResultSet but got %s"
                        .formatted(rresultSet.getClass().getName()));
            }
            ArrowJoinResultReader.emit(duckDbResultSet, emitter, source);
            source.complete();
            emitter.complete();
        } catch (Exception e) {
            // Catch-all (SQLException, IOException, IllegalStateException from schema mismatch, any
            // runtime failure from Arrow decoding) so every failure is routed through emitter.error
            // rather than some through emitter.error and others unwinding up to the bridge's
            // startSource catch-all.
            emitter.error(new IllegalStateException("DuckDB join query failed: %s".formatted(e.getMessage()), e));
        }
    }

    private static String buildJoinSql(boolean leftJoin, int keyArity) {
        var keyCols = IntStream.range(0, keyArity).mapToObj("k%d"::formatted).toList();
        var onClause = keyCols.stream().map(k -> "c.%s = p.%s".formatted(k, k)).collect(Collectors.joining(" AND "));
        var joinKind = leftJoin ? "LEFT" : "INNER";
        // FILTER excludes the artificial NULL row produced by LEFT JOIN's no-match case so the
        // returned LIST is empty (rather than [NULL]) when a child has no parent. GROUP BY on both
        // c.row_id AND c.blob is required: DuckDB's aggregate semantics require c.blob in the
        // GROUP BY for it to be projected, and c.row_id is not a declared PK so functional
        // dependency optimization doesn't apply.
        return ("SELECT c.row_id, c.blob, list(p.blob ORDER BY p.row_id) FILTER (WHERE p.row_id IS NOT NULL)"
                        + " FROM %s c %s JOIN %s p ON %s GROUP BY c.row_id, c.blob ORDER BY c.row_id")
                .formatted(CHILD_TABLE, joinKind, PARENT_TABLE, onClause);
    }

    private void openConnection() {
        try {
            String jdbcUrl;
            if (fileBacked) {
                workingDir = Files.createTempDirectory(spillDir, "carml-spill-");
                dbFile = workingDir.resolve("join-" + UUID.randomUUID() + ".duckdb");
                jdbcUrl = "jdbc:duckdb:" + dbFile;
            } else {
                jdbcUrl = "jdbc:duckdb:";
            }
            connection = DriverManager.getConnection(jdbcUrl);
            if (fileBacked) {
                try (var stmt = connection.createStatement()) {
                    // DuckDB SQL literals use '' to escape single quotes — defensive for paths
                    // containing ' (rare but possible).
                    var escaped = workingDir.toAbsolutePath().toString().replace("'", "''");
                    stmt.execute("SET temp_directory = '%s'".formatted(escaped));
                }
            }
        } catch (SQLException | IOException e) {
            throw new IllegalStateException("Failed to open DuckDB join connection", e);
        }
    }

    @SuppressWarnings({"java:S2077", "SqlSourceToSinkFlow"}) // Table name is an internal constant
    // (PARENT_TABLE / CHILD_TABLE) and key arity is bounded by the join definition. No user input
    // reaches this SQL. DDL has no PreparedStatement-parameter form for table names / column lists.
    private void createTempTable(String tableName, int keyArity) {
        var keyCols =
                IntStream.range(0, keyArity).mapToObj("k%d VARCHAR"::formatted).collect(Collectors.joining(", "));
        var sql = "CREATE TEMP TABLE %s (row_id INTEGER, %s, blob BLOB)".formatted(tableName, keyCols);
        try (var stmt = connection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create temp table %s".formatted(tableName), e);
        }
    }

    private DuckDBAppender createAppender(String tableName) {
        try {
            return ((DuckDBConnection) connection).createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create appender for %s".formatted(tableName), e);
        }
    }

    private void appendParent(
            ViewIteration parent, List<Join> conditions, Set<String> parentReferenceableKeys, int keyArity) {
        var key = JoinKeyExtractor.parentKey(conditions, parent, parentReferenceableKeys, evaluator);
        // Skip parents whose join key did not produce a value — they cannot match any child key,
        // matching the in-memory path semantics where the HashMapJoinIndex never stores them.
        if (key.isEmpty()) {
            parentRowId++;
            return;
        }
        appendRow(parentAppender, parentRowId++, key, ArrowBlobCodec.encodeParent(parent), keyArity);
    }

    private static void appendRow(DuckDBAppender appender, int rowId, List<Object> key, byte[] blob, int keyArity) {
        try {
            appender.beginRow();
            appender.append(rowId);
            for (int i = 0; i < keyArity; i++) {
                if (i < key.size()) {
                    appender.append(key.get(i).toString());
                } else {
                    appender.appendNull();
                }
            }
            appender.append(blob);
            appender.endRow();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to append row to DuckDB appender", e);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (parentAppender != null) {
            try {
                parentAppender.close();
            } catch (SQLException e) {
                LOG.debug("Failed to close parent appender on cleanup", e);
            }
            parentAppender = null;
        }
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOG.warn("Failed to close DuckDB join connection: {}", e.getMessage());
            }
            connection = null;
        }
        if (workingDir != null) {
            deleteWorkingDir();
        }
    }

    private void deleteWorkingDir() {
        try (var entries = Files.walk(workingDir)) {
            entries.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    LOG.warn("Failed to delete spill file {}: {}", p, e.getMessage());
                }
            });
        } catch (IOException e) {
            LOG.warn("Failed to walk spill directory {}: {}", workingDir, e.getMessage());
        }
        workingDir = null;
        dbFile = null;
    }

    Path getDbFile() {
        return dbFile;
    }

    Path getWorkingDir() {
        return workingDir;
    }
}
