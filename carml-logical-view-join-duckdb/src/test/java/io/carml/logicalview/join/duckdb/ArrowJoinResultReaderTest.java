package io.carml.logicalview.join.duckdb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.carml.logicalsourceresolver.PausableFluxBridge;
import io.carml.logicalview.EvaluatedValues;
import io.carml.logicalview.MatchedRow;
import io.carml.logicalview.ViewIteration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Unit tests for {@link ArrowJoinResultReader}. Each test wires a real in-memory DuckDB connection
 * with the same schema {@code DuckDbJoinExecutor} uses, executes the real join SQL, and drives the
 * reader through a real {@link PausableFluxBridge} so the emitter / pausable-source integration is
 * also exercised.
 */
class ArrowJoinResultReaderTest {

    private static final String PARENT_TABLE = "parents";
    private static final String CHILD_TABLE = "children";

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void emit_innerJoinSingleMatch_emitsOneRowWithOneParent() {
        createTempTable(PARENT_TABLE);
        createTempTable(CHILD_TABLE);

        var parent = ViewIteration.of(0, Map.of("pid", "k", "name", "alpha"), Map.of(), Map.of());
        var child = new EvaluatedValues(Map.of("cid", "k"), Map.of(), Map.of());
        appendParent(0, "k", ArrowBlobCodec.encodeParent(parent));
        appendChild(0, "k", ArrowBlobCodec.encodeChild(child));

        var rows = runJoinAndCollect(false);

        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).matchedParents(), hasSize(1));
        assertThat(rows.get(0).child().values().get("cid").toString(), is("k"));
        assertThat(
                rows.get(0)
                        .matchedParents()
                        .get(0)
                        .getValue("name")
                        .orElseThrow()
                        .toString(),
                is("alpha"));
    }

    @Test
    void emit_innerJoinMultipleParents_preservesParentRowIdOrder() {
        createTempTable(PARENT_TABLE);
        createTempTable(CHILD_TABLE);

        // Three parents sharing the same key, appended in order row_id=0,1,2.
        for (int i = 0; i < 3; i++) {
            var parent = ViewIteration.of(i, Map.of("pid", "k", "name", "p" + i), Map.of(), Map.of());
            appendParent(i, "k", ArrowBlobCodec.encodeParent(parent));
        }
        var child = new EvaluatedValues(Map.of("cid", "k"), Map.of(), Map.of());
        appendChild(0, "k", ArrowBlobCodec.encodeChild(child));

        var rows = runJoinAndCollect(false);

        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).matchedParents(), hasSize(3));
        var names = rows.get(0).matchedParents().stream()
                .map(p -> p.getValue("name").orElseThrow().toString())
                .toList();
        assertThat(names, contains("p0", "p1", "p2"));
    }

    @Test
    void emit_leftJoinNoMatch_childStillEmittedWithEmptyParents() {
        createTempTable(PARENT_TABLE);
        createTempTable(CHILD_TABLE);

        var parent = ViewIteration.of(0, Map.of("pid", "other"), Map.of(), Map.of());
        appendParent(0, "other", ArrowBlobCodec.encodeParent(parent));
        var child = new EvaluatedValues(Map.of("cid", "no-match"), Map.of(), Map.of());
        appendChild(0, "no-match", ArrowBlobCodec.encodeChild(child));

        var rows = runJoinAndCollect(true);

        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).matchedParents(), is(empty()));
        assertThat(rows.get(0).child().values().get("cid").toString(), is("no-match"));
    }

    @Test
    void emit_noResultRows_completesWithoutEmitting() {
        createTempTable(PARENT_TABLE);
        createTempTable(CHILD_TABLE);

        // Empty child table → even a LEFT JOIN produces zero result rows.
        var rows = runJoinAndCollect(true);

        assertThat(rows, is(empty()));
    }

    @Test
    void emit_cancellationAfterFirstRow_stopsEmittingFurtherRows() {
        createTempTable(PARENT_TABLE);
        createTempTable(CHILD_TABLE);

        // 10 children each matching a distinct parent.
        for (int i = 0; i < 10; i++) {
            var parent = ViewIteration.of(i, Map.of("pid", String.valueOf(i)), Map.of(), Map.of());
            appendParent(i, String.valueOf(i), ArrowBlobCodec.encodeParent(parent));
            var child = new EvaluatedValues(Map.of("cid", String.valueOf(i)), Map.of(), Map.of());
            appendChild(i, String.valueOf(i), ArrowBlobCodec.encodeChild(child));
        }

        var sql = buildJoinSql(false);

        // Wire the real bridge and trigger cancel via take(1). The cancel propagates through
        // onDispose → source.cancel(), which the reader observes on its next row iteration.
        var sourceRef = new AtomicReference<DuckDbPausableSource>();
        var flux = PausableFluxBridge.<MatchedRow>builder()
                .sourceFactory(emitter -> {
                    var source = new DuckDbPausableSource(self -> runEmit(sql, emitter, self));
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

        var firstOnly = flux.take(1).collectList().block();
        assertThat(firstOnly, hasSize(1));
    }

    @Test
    void emit_acrossMultipleArrowBatches_emitsAllRowsInOrder() {
        createTempTable(PARENT_TABLE);
        createTempTable(CHILD_TABLE);

        // 5000 rows > the 2048 Arrow batch size → exercises the batch loop across boundaries.
        int n = 5_000;
        for (int i = 0; i < n; i++) {
            var parent = ViewIteration.of(i, Map.of("pid", String.valueOf(i), "tag", "p" + i), Map.of(), Map.of());
            appendParent(i, String.valueOf(i), ArrowBlobCodec.encodeParent(parent));
            var child = new EvaluatedValues(Map.of("cid", String.valueOf(i)), Map.of(), Map.of());
            appendChild(i, String.valueOf(i), ArrowBlobCodec.encodeChild(child));
        }

        var rows = runJoinAndCollect(false);

        assertThat(rows, hasSize(n));
        // Child rows come back in ORDER BY c.row_id — matches insertion order.
        for (int i = 0; i < n; i++) {
            assertThat(rows.get(i).child().values().get("cid").toString(), is(String.valueOf(i)));
            assertThat(rows.get(i).matchedParents(), hasSize(1));
            assertThat(
                    rows.get(i)
                            .matchedParents()
                            .get(0)
                            .getValue("tag")
                            .orElseThrow()
                            .toString(),
                    is("p" + i));
        }
    }

    @Test
    void emit_schemaMismatchMidStream_routesErrorToSubscriberWithoutLeakingArrowMemory() {
        // Drives a query whose column 1 is INTEGER instead of BLOB. The per-batch cast to
        // VarBinaryVector in ArrowJoinResultReader.emitBatches throws IllegalStateException
        // mid-batch — AFTER RootAllocator and ArrowReader have been opened. RootAllocator.close()
        // throws "Memory was leaked" if any child buffer wasn't released; if that happened the
        // subscriber would see that message instead of the schema-mismatch one. Seeing the
        // schema-mismatch error confirms the try-with-resources on allocator + reader both closed
        // cleanly on the exception path.
        var sql = "SELECT 0 AS row_id, 42 AS blob, [43, 44] AS parents";
        var sourceRef = new AtomicReference<DuckDbPausableSource>();
        var flux = PausableFluxBridge.<MatchedRow>builder()
                .sourceFactory(emitter -> {
                    var source = new DuckDbPausableSource(self -> {
                        try (var stmt = connection.createStatement();
                                var rs = stmt.executeQuery(sql)) {
                            if (rs instanceof DuckDBResultSet duckDbRs) {
                                ArrowJoinResultReader.emit(duckDbRs, emitter, self);
                            } else {
                                emitter.error(new IllegalStateException("Expected DuckDBResultSet"));
                            }
                        } catch (Exception e) {
                            emitter.error(e);
                        }
                    });
                    sourceRef.set(source);
                    return source;
                })
                .onDispose(() -> {
                    var s = sourceRef.get();
                    if (s != null) {
                        s.cancel();
                    }
                })
                .flux();

        // The "VarBinaryVector" substring mirrors the exception message template in
        // emitBatches; if Arrow upgrades rename or relocate the vector class, update the message
        // template and this assertion in lockstep.
        StepVerifier.create(flux)
                .expectErrorMatches(err -> err instanceof IllegalStateException
                        && err.getMessage() != null
                        && err.getMessage().contains("VarBinaryVector"))
                .verify(Duration.ofSeconds(10));
    }

    // --- helpers -------------------------------------------------------------------------------

    private List<MatchedRow> runJoinAndCollect(boolean leftJoin) {
        var sql = buildJoinSql(leftJoin);
        var sourceRef = new AtomicReference<DuckDbPausableSource>();
        var flux = PausableFluxBridge.<MatchedRow>builder()
                .sourceFactory(emitter -> {
                    var source = new DuckDbPausableSource(self -> runEmit(sql, emitter, self));
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
        return Flux.from(flux).collectList().block();
    }

    private void runEmit(String sql, PausableFluxBridge.Emitter<MatchedRow> emitter, DuckDbPausableSource source) {
        try (var stmt = connection.createStatement();
                var rs = stmt.executeQuery(sql)) {
            if (!(rs instanceof DuckDBResultSet duckDbRs)) {
                emitter.error(new IllegalStateException(
                        "Expected DuckDBResultSet, got " + rs.getClass().getName()));
                return;
            }
            ArrowJoinResultReader.emit(duckDbRs, emitter, source);
            source.complete();
            emitter.complete();
        } catch (Exception e) {
            emitter.error(e);
        }
    }

    private static String buildJoinSql(boolean leftJoin) {
        // Mirrors DuckDbJoinExecutor.buildJoinSql for keyArity=1.
        var joinKind = leftJoin ? "LEFT" : "INNER";
        return "SELECT c.row_id, c.blob, list(p.blob ORDER BY p.row_id) FILTER (WHERE p.row_id IS NOT NULL)"
                + " FROM " + CHILD_TABLE + " c " + joinKind + " JOIN " + PARENT_TABLE + " p ON c.k0 = p.k0"
                + " GROUP BY c.row_id, c.blob ORDER BY c.row_id";
    }

    @SuppressWarnings({"java:S2077"}) // Internal test DDL, no user input.
    private void createTempTable(String tableName) {
        var sql = "CREATE TEMP TABLE " + tableName + " (row_id INTEGER, k0 VARCHAR, blob BLOB)";
        try (var stmt = connection.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create temp table " + tableName, e);
        }
    }

    private void appendParent(int rowId, String key, byte[] blob) {
        appendRow(PARENT_TABLE, rowId, key, blob);
    }

    private void appendChild(int rowId, String key, byte[] blob) {
        appendRow(CHILD_TABLE, rowId, key, blob);
    }

    private void appendRow(String tableName, int rowId, String key, byte[] blob) {
        try (DuckDBAppender appender =
                ((DuckDBConnection) connection).createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName)) {
            appender.beginRow();
            appender.append(rowId);
            appender.append(key);
            appender.append(blob);
            appender.endRow();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to append row to " + tableName, e);
        }
    }
}
