package io.carml.logicalview.join.duckdb;

import io.carml.logicalsourceresolver.PausableFluxBridge;
import io.carml.logicalview.MatchedRow;
import io.carml.logicalview.ViewIteration;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBResultSet;

/**
 * Reads DuckDB join result rows via the Arrow C Data Interface and emits them as
 * {@link MatchedRow}s.
 *
 * <p>Uses {@link DuckDBResultSet#arrowExportStream(Object, long)} to pull batches (columnar
 * vectors, default 2048 rows) from DuckDB with zero-copy from native memory, eliminating the
 * per-cell JNI overhead of JDBC {@code ResultSet.getObject()} / {@code getArray()}. Each row's
 * child and parent payloads are then decoded via {@link ArrowBlobCodec}.
 *
 * <p>The input result set schema is fixed by {@code DuckDbJoinExecutor.buildJoinSql}: three
 * columns, where column 0 is an unused {@code INTEGER} (present only to satisfy {@code GROUP BY}
 * and {@code ORDER BY}), column 1 holds the child {@code BLOB}, and column 2 holds the ordered
 * parent {@code LIST<BLOB>}. The reader does not inspect column names — positional access is
 * deliberate since the SQL is an internal constant. If Arrow export fails or returns an
 * unexpected type, the reader throws: this module is Arrow-only and a silent JDBC fallback would
 * mask a real environmental problem.
 */
final class ArrowJoinResultReader {

    /**
     * Arrow batch size matching DuckDB's internal vector size (2048), to avoid partial-batch
     * overhead.
     */
    private static final long BATCH_SIZE = 2048L;

    private ArrowJoinResultReader() {}

    /**
     * Streams join result rows from the given {@link DuckDBResultSet} through {@code emitter},
     * honoring cancellation and backpressure via {@code source}.
     *
     * <p>The caller is responsible for calling {@code source.complete()} / {@code emitter.complete()}
     * on the non-canceled happy path, and for routing exceptions to {@code emitter.error(...)}. On
     * cancellation this method returns early without completing anything.
     */
    static void emit(
            DuckDBResultSet resultSet, PausableFluxBridge.Emitter<MatchedRow> emitter, DuckDbPausableSource source)
            throws IOException, SQLException {
        try (var allocator = new RootAllocator()) {
            var readerObj = resultSet.arrowExportStream(allocator, BATCH_SIZE);
            if (!(readerObj instanceof ArrowReader reader)) {
                // Unexpected return type. Close defensively in case it holds a native stream handle,
                // then surface the schema mismatch.
                closeQuietly(readerObj);
                throw new IllegalStateException("Expected ArrowReader from arrowExportStream but got %s"
                        .formatted(
                                readerObj == null
                                        ? "null"
                                        : readerObj.getClass().getName()));
            }
            try (reader) {
                emitBatches(reader, emitter, source);
            }
        }
    }

    private static void closeQuietly(Object maybeCloseable) {
        if (maybeCloseable instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // Best-effort cleanup on the unexpected-type path; the primary exception carries the
                // real signal.
            }
        }
    }

    private static void emitBatches(
            ArrowReader reader, PausableFluxBridge.Emitter<MatchedRow> emitter, DuckDbPausableSource source)
            throws IOException {
        while (reader.loadNextBatch()) {
            var root = reader.getVectorSchemaRoot();
            var rowCount = root.getRowCount();
            if (rowCount == 0) {
                continue;
            }
            var vectors = resolveBatchVectors(root);
            for (var row = 0; row < rowCount; row++) {
                if (source.isCancelled()) {
                    return;
                }
                emitter.next(readRow(row, vectors.child(), vectors.parentList(), vectors.parentInner()));
                source.awaitDemand();
            }
        }
    }

    private static BatchVectors resolveBatchVectors(VectorSchemaRoot root) {
        var vectors = root.getFieldVectors();
        if (vectors.size() < 3) {
            throw new IllegalStateException(
                    "Expected at least 3 columns in join result but got %d".formatted(vectors.size()));
        }
        var childVectorRaw = vectors.get(1);
        if (!(childVectorRaw instanceof VarBinaryVector childVec)) {
            throw new IllegalStateException("Expected VarBinaryVector for child BLOB column but got %s"
                    .formatted(childVectorRaw.getClass().getName()));
        }
        var parentVectorRaw = vectors.get(2);
        if (!(parentVectorRaw instanceof ListVector parentListVec)) {
            throw new IllegalStateException("Expected ListVector for parent BLOB list column but got %s"
                    .formatted(parentVectorRaw.getClass().getName()));
        }
        var parentInnerRaw = parentListVec.getDataVector();
        if (!(parentInnerRaw instanceof VarBinaryVector parentInnerVec)) {
            throw new IllegalStateException("Expected VarBinaryVector for inner parent BLOB elements but got %s"
                    .formatted(parentInnerRaw.getClass().getName()));
        }
        return new BatchVectors(childVec, parentListVec, parentInnerVec);
    }

    private record BatchVectors(VarBinaryVector child, ListVector parentList, VarBinaryVector parentInner) {}

    private static MatchedRow readRow(
            int row, VarBinaryVector childVec, ListVector parentListVec, VarBinaryVector parentInnerVec) {
        if (childVec.isNull(row)) {
            throw new IllegalStateException(
                    ("Unexpected null child BLOB at row %d — join result schema requires every child row to"
                                    + " carry a payload")
                            .formatted(row));
        }
        var child = ArrowBlobCodec.decodeChild(childVec.get(row));

        List<ViewIteration> parents;
        if (parentListVec.isNull(row)) {
            parents = List.of();
        } else {
            var start = parentListVec.getElementStartIndex(row);
            var end = parentListVec.getElementEndIndex(row);
            if (start == end) {
                parents = List.of();
            } else {
                parents = new ArrayList<>(end - start);
                for (var inner = start; inner < end; inner++) {
                    // The join SQL uses FILTER (WHERE p.row_id IS NOT NULL) so null inner
                    // elements should not occur. Guard defensively to mirror the previous
                    // JDBC-path null-check and avoid a hard failure on an unexpected null.
                    if (!parentInnerVec.isNull(inner)) {
                        parents.add(ArrowBlobCodec.decodeParent(parentInnerVec.get(inner)));
                    }
                }
            }
        }
        return new MatchedRow(child, parents);
    }
}
