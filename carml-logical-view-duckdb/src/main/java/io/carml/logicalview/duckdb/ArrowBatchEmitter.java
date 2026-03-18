package io.carml.logicalview.duckdb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBResultSet;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import reactor.core.publisher.FluxSink;

/**
 * Emits {@link DuckDbViewIteration}s from a DuckDB {@link java.sql.ResultSet} using Apache Arrow
 * batch transfer instead of JDBC row-by-row iteration.
 *
 * <p>DuckDB's JDBC driver provides {@link DuckDBResultSet#arrowExportStream(Object, long)} which
 * exports query results as Arrow IPC batches via the C Data Interface. This transfers data in
 * columnar batches (typically 2048 rows) with zero copy from native DuckDB memory, eliminating the
 * per-cell JNI overhead of {@code ResultSet.getObject()}.
 *
 * <p>Arrow vectors return Java objects whose types may differ from JDBC's. This emitter normalizes
 * all values to match the types produced by the JDBC path, ensuring that downstream consumers
 * (including {@link DuckDbViewIteration}) see identical data regardless of which transfer method was
 * used.
 */
@Slf4j
final class ArrowBatchEmitter {

    private static final String INDEX_KEY = DuckDbLogicalViewEvaluator.INDEX_KEY;

    private static final String ORDINAL_SUFFIX = DuckDbLogicalViewEvaluator.INDEX_KEY_SUFFIX;

    /**
     * Default Arrow batch size. DuckDB's internal vector size is 2048, so we match it to avoid
     * partial batch overhead.
     */
    private static final long ARROW_BATCH_SIZE = 2048;

    private ArrowBatchEmitter() {}

    /**
     * Attempts to emit rows from the given {@link DuckDBResultSet} using Arrow batch transfer.
     *
     * @return {@code true} if Arrow emission succeeded, {@code false} if Arrow is not available
     *     and the caller should fall back to JDBC
     * @throws RuntimeException if Arrow started transferring but failed mid-stream — fallback
     *     would produce corrupt output since the ResultSet is consumed
     */
    static boolean tryEmitArrowBatches(
            FluxSink<? super DuckDbViewIteration> sink,
            DuckDBResultSet duckDbResultSet,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            CompiledView compiledView,
            Set<String> referenceableKeys,
            boolean retainSourceEvaluation) {
        try (var allocator = new RootAllocator()) {
            return emitArrowBatches(
                    sink, duckDbResultSet, allocator, columns, compiledView, referenceableKeys, retainSourceEvaluation);
        } catch (NoClassDefFoundError | UnsupportedOperationException e) {
            LOG.debug("Arrow transfer not available, falling back to JDBC: {}", e.getMessage());
            return false;
        }
    }

    private static boolean emitArrowBatches(
            FluxSink<? super DuckDbViewIteration> sink,
            DuckDBResultSet duckDbResultSet,
            BufferAllocator allocator,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            CompiledView compiledView,
            Set<String> referenceableKeys,
            boolean retainSourceEvaluation) {
        // Phase 1: Try to obtain the Arrow reader. If this fails, JDBC fallback is safe
        // because no data has been consumed from the ResultSet yet.
        ArrowReader arrowReader;
        try {
            var arrowReaderObj = duckDbResultSet.arrowExportStream(allocator, ARROW_BATCH_SIZE);
            if (!(arrowReaderObj instanceof ArrowReader reader)) {
                LOG.debug(
                        "arrowExportStream returned unexpected type: {}",
                        arrowReaderObj.getClass().getName());
                return false;
            }
            arrowReader = reader;
        } catch (SQLException e) {
            LOG.debug("Arrow export failed, falling back to JDBC: {}", e.getMessage());
            return false;
        }

        // Phase 2: Read batches. Once we start reading, the ResultSet is consumed and
        // JDBC fallback would produce corrupt output. Any exception here must propagate.
        try (arrowReader) {
            emitFromReader(sink, arrowReader, columns, compiledView, referenceableKeys, retainSourceEvaluation);
        } catch (IOException e) {
            throw new ArrowTransferException("Arrow batch reading failed after data transfer started", e);
        }

        LOG.debug("Arrow batch transfer completed successfully");
        return true;
    }

    private static void emitFromReader(
            FluxSink<? super DuckDbViewIteration> sink,
            ArrowReader arrowReader,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            CompiledView compiledView,
            Set<String> referenceableKeys,
            boolean retainSourceEvaluation)
            throws IOException {
        while (arrowReader.loadNextBatch()) {
            var root = arrowReader.getVectorSchemaRoot();
            var rowCount = root.getRowCount();
            if (rowCount == 0) {
                continue;
            }

            // Build column name-to-vector map once per batch for O(1) column lookup
            var columnIndex = buildColumnIndex(root);

            for (var row = 0; row < rowCount; row++) {
                if (sink.isCancelled()) {
                    return;
                }

                var zeroBasedIndex = readZeroBasedIndex(columnIndex, columns, row);
                var values = readColumnValues(columnIndex, columns, zeroBasedIndex, row);

                validateNoNonScalarTypes(columnIndex, columns, compiledView, row);

                var naturalDatatypes = resolveNaturalDatatypes(columnIndex, columns, values, row);
                var sourceEvaluation = resolveSourceEvaluation(columnIndex, columns, retainSourceEvaluation, row);

                sink.next(new DuckDbViewIteration(
                        zeroBasedIndex, values, naturalDatatypes, sourceEvaluation, referenceableKeys));
            }
        }
    }

    /**
     * Builds a name-to-vector map for fast column lookup within a batch. This avoids repeated
     * linear scans of the schema for each row.
     */
    private static Map<String, FieldVector> buildColumnIndex(VectorSchemaRoot root) {
        var index =
                new LinkedHashMap<String, FieldVector>(root.getFieldVectors().size());
        for (var vector : root.getFieldVectors()) {
            index.put(vector.getName(), vector);
        }
        return index;
    }

    private static int readZeroBasedIndex(
            Map<String, FieldVector> columnIndex, DuckDbLogicalViewEvaluator.ColumnDescriptor columns, int row) {
        if (columns.idxColumnName() != null) {
            var idxVector = columnIndex.get(columns.idxColumnName());
            if (idxVector != null && !idxVector.isNull(row)) {
                var rawIndex = ((Number) idxVector.getObject(row)).intValue();
                return rawIndex > 0 ? rawIndex - 1 : 0;
            }
        }
        return 0;
    }

    private static LinkedHashMap<String, Object> readColumnValues(
            Map<String, FieldVector> columnIndex,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            int zeroBasedIndex,
            int row) {
        var values = new LinkedHashMap<String, Object>(columns.valueNames().size() + 1);
        values.put(INDEX_KEY, zeroBasedIndex);
        for (var colName : columns.valueNames()) {
            values.put(colName, readColumnValue(columnIndex, colName, columns, row));
        }
        return values;
    }

    private static Object readColumnValue(
            Map<String, FieldVector> columnIndex,
            String colName,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            int row) {
        var vector = columnIndex.get(colName);
        if (vector == null || vector.isNull(row)) {
            return null;
        }

        var rawValue = vector.getObject(row);

        // Normalize Arrow types to match JDBC behavior
        rawValue = normalizeArrowValue(rawValue);

        // Convert LIST columns to Java Lists for aggregated join fields (columns without type
        // companions). Columns with type companions are source data fields where lists indicate
        // non-scalar values.
        if (rawValue instanceof List<?> listValue
                && !columns.typeNames().contains(colName + DuckDbSourceStrategy.TYPE_SUFFIX)) {
            return normalizeListElements(listValue);
        }

        return rawValue;
    }

    /**
     * Normalizes Arrow-specific types to standard Java types matching JDBC behavior.
     *
     * <p>Arrow vectors return types like {@code org.apache.arrow.vector.util.Text} for VARCHAR
     * columns, whereas JDBC returns {@code java.lang.String}. This method ensures type
     * compatibility.
     */
    private static Object normalizeArrowValue(Object value) {
        if (value == null) {
            return null;
        }
        // Arrow VarCharVector returns org.apache.arrow.vector.util.Text; convert to String
        if (value instanceof org.apache.arrow.vector.util.Text text) {
            return text.toString();
        }
        // Arrow JsonStringHashMap (for STRUCT) and other complex types are returned as-is
        return value;
    }

    /**
     * Normalizes list elements, converting any Arrow-specific types within the list to standard
     * Java types.
     */
    private static List<Object> normalizeListElements(List<?> listValue) {
        var normalized = new ArrayList<>(listValue.size());
        for (var element : listValue) {
            normalized.add(normalizeArrowValue(element));
        }
        return normalized;
    }

    private static void validateNoNonScalarTypes(
            Map<String, FieldVector> columnIndex,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            CompiledView compiledView,
            int row) {
        var nonScalarTypeValues = compiledView.nonScalarTypeValues();
        if (nonScalarTypeValues.isEmpty()) {
            return;
        }

        var multiValuedFieldNames = compiledView.multiValuedFieldNames();
        for (var typeCol : columns.typeNames()) {
            var fieldName = typeCol.substring(0, typeCol.length() - DuckDbSourceStrategy.TYPE_SUFFIX.length());

            if (multiValuedFieldNames.contains(fieldName)) {
                continue;
            }

            var typeVector = columnIndex.get(typeCol);
            if (typeVector != null && !typeVector.isNull(row)) {
                var duckDbType = normalizeArrowValue(typeVector.getObject(row));
                if (duckDbType != null && nonScalarTypeValues.contains(duckDbType.toString())) {
                    throw new IllegalArgumentException(
                            "Reference '%s' resolves to a non-scalar value (type: %s). Per the RML spec, references must resolve to scalar values."
                                    .formatted(fieldName, duckDbType));
                }
            }
        }
    }

    private static Map<String, IRI> resolveNaturalDatatypes(
            Map<String, FieldVector> columnIndex,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            Map<String, Object> values,
            int row) {
        var naturalDatatypes = new LinkedHashMap<String, IRI>();

        naturalDatatypes.put(INDEX_KEY, XSD.INTEGER);

        for (var colName : values.keySet()) {
            if (colName.endsWith(ORDINAL_SUFFIX)) {
                naturalDatatypes.put(colName, XSD.INTEGER);
            }
        }

        for (var typeCol : columns.typeNames()) {
            var typeVector = columnIndex.get(typeCol);
            if (typeVector != null && !typeVector.isNull(row)) {
                var duckDbType = normalizeArrowValue(typeVector.getObject(row));
                if (duckDbType != null) {
                    var xsdType = DuckDbLogicalViewEvaluator.resolveXsdType(duckDbType.toString());
                    if (xsdType != null) {
                        var fieldName =
                                typeCol.substring(0, typeCol.length() - DuckDbSourceStrategy.TYPE_SUFFIX.length());
                        naturalDatatypes.put(fieldName, xsdType);
                    }
                }
            }
        }

        return naturalDatatypes;
    }

    private static io.carml.logicalsourceresolver.ExpressionEvaluation resolveSourceEvaluation(
            Map<String, FieldVector> columnIndex,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            boolean retainSourceEvaluation,
            int row) {
        if (retainSourceEvaluation && columns.iterColumnName() != null) {
            var iterVector = columnIndex.get(columns.iterColumnName());
            if (iterVector != null && !iterVector.isNull(row)) {
                var rawJson = normalizeArrowValue(iterVector.getObject(row));
                if (rawJson != null) {
                    return new DuckDbJsonSourceEvaluation(rawJson.toString());
                }
            }
        }
        return null;
    }
}
