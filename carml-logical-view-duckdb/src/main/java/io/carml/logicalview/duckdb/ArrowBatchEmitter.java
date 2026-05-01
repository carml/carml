package io.carml.logicalview.duckdb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Deque;
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

/**
 * Materializes {@link DuckDbViewIteration}s from a DuckDB {@link java.sql.ResultSet} using Apache
 * Arrow batch transfer instead of JDBC row-by-row iteration.
 *
 * <p>DuckDB's JDBC driver provides {@link DuckDBResultSet#arrowExportStream(Object, long)} which
 * exports query results as Arrow IPC batches via the C Data Interface. This transfers data in
 * columnar batches (typically 2048 rows) with zero copy from native DuckDB memory, eliminating the
 * per-cell JNI overhead of {@code ResultSet.getObject()}.
 *
 * <p>Arrow vectors return Java objects whose types may differ from JDBC's. This class normalizes
 * all values to match the types produced by the JDBC path, ensuring that downstream consumers
 * (including {@link DuckDbViewIteration}) see identical data regardless of which transfer method
 * was used.
 *
 * <p>Use {@link #tryOpen} to construct a {@link DuckDbViewIterator.BatchLoader} that loads one
 * Arrow batch per {@code loadInto} call. This pull-based pattern (consumed by
 * {@link DuckDbViewIterator}) gives natural backpressure: items are produced only when the
 * downstream Reactor subscriber requests more.
 */
@Slf4j
final class ArrowBatchEmitter {

    private static final String INDEX_KEY = DuckDbLogicalViewEvaluator.INDEX_KEY;

    private static final String ORDINAL_SUFFIX = DuckDbLogicalViewEvaluator.INDEX_KEY_SUFFIX;

    /**
     * Default Arrow batch size. DuckDB's internal vector size is 2048, so we match it to avoid
     * partial batch overhead.
     */
    static final long ARROW_BATCH_SIZE = 2048;

    private ArrowBatchEmitter() {}

    /**
     * Attempts to open an Arrow-backed {@link DuckDbViewIterator.BatchLoader} for the given result
     * set. Returns {@code null} if Arrow transfer is unavailable (e.g. classpath missing
     * arrow-memory-netty), in which case the caller should fall back to JDBC row-by-row reading.
     *
     * <p>If this method returns a non-null loader, the result set has been consumed by the Arrow
     * reader and JDBC fallback is no longer safe. The returned loader takes ownership of an
     * allocator and the underlying {@link ArrowReader}; both are released by
     * {@link DuckDbViewIterator.BatchLoader#close()}.
     */
    static DuckDbViewIterator.BatchLoader tryOpen(
            DuckDBResultSet duckDbResultSet,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            CompiledView compiledView,
            Set<String> referenceableKeys,
            boolean retainSourceEvaluation) {
        BufferAllocator allocator;
        try {
            allocator = new RootAllocator();
        } catch (NoClassDefFoundError | UnsupportedOperationException e) {
            logArrowUnavailable(e);
            return null;
        }

        ArrowReader arrowReader;
        try {
            var arrowReaderObj = duckDbResultSet.arrowExportStream(allocator, ARROW_BATCH_SIZE);
            if (!(arrowReaderObj instanceof ArrowReader reader)) {
                LOG.debug(
                        "arrowExportStream returned unexpected type: {}",
                        arrowReaderObj.getClass().getName());
                allocator.close();
                return null;
            }
            arrowReader = reader;
        } catch (SQLException e) {
            LOG.debug("Arrow export failed, falling back to JDBC: {}", e.getMessage());
            allocator.close();
            return null;
        } catch (NoClassDefFoundError | UnsupportedOperationException e) {
            logArrowUnavailable(e);
            allocator.close();
            return null;
        }

        return new ArrowBatchLoader(
                allocator, arrowReader, columns, compiledView, referenceableKeys, retainSourceEvaluation);
    }

    private static void logArrowUnavailable(Throwable cause) {
        LOG.debug("Arrow transfer not available, falling back to JDBC: {}", cause.getMessage());
    }

    private record ArrowBatchLoader(
            BufferAllocator allocator,
            ArrowReader arrowReader,
            DuckDbLogicalViewEvaluator.ColumnDescriptor columns,
            CompiledView compiledView,
            Set<String> referenceableKeys,
            boolean retainSourceEvaluation)
            implements DuckDbViewIterator.BatchLoader {

        @Override
        public boolean loadInto(Deque<DuckDbViewIteration> buffer) {
            try {
                if (!arrowReader.loadNextBatch()) {
                    return false;
                }
                var root = arrowReader.getVectorSchemaRoot();
                var rowCount = root.getRowCount();
                if (rowCount == 0) {
                    // Empty batch — caller's loop retries via DuckDbViewIterator.hasNext.
                    return true;
                }

                var columnIndex = buildColumnIndex(root);
                for (var row = 0; row < rowCount; row++) {
                    var zeroBasedIndex = readZeroBasedIndex(columnIndex, columns, row);
                    var values = readColumnValues(columnIndex, columns, zeroBasedIndex, row);
                    validateNoNonScalarTypes(columnIndex, columns, compiledView, row);
                    var naturalDatatypes = resolveNaturalDatatypes(columnIndex, columns, values, row);
                    var sourceEvaluation = resolveSourceEvaluation(columnIndex, columns, retainSourceEvaluation, row);

                    buffer.offer(DuckDbViewIteration.ofOwnedMaps(
                            zeroBasedIndex, values, naturalDatatypes, sourceEvaluation, referenceableKeys));
                }
                return true;
            } catch (IOException e) {
                throw new ArrowTransferException("Arrow batch reading failed after data transfer started", e);
            }
        }

        @Override
        public void close() {
            try {
                arrowReader.close();
            } catch (Exception e) {
                LOG.debug("Error closing Arrow reader", e);
            }
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.debug("Error closing Arrow allocator", e);
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
                    throw new IllegalArgumentException(("Reference '%s' resolves to a non-scalar value (type: %s)."
                                    + " Per the RML spec, references must resolve to scalar values.")
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
