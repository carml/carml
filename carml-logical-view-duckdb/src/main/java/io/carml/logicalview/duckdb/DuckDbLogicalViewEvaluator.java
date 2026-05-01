package io.carml.logicalview.duckdb;

import io.carml.logicalsourceresolver.ExpressionEvaluation;
import io.carml.logicalsourceresolver.ResolvedSource;
import io.carml.logicalview.DefaultLogicalViewEvaluator;
import io.carml.logicalview.EvaluationContext;
import io.carml.logicalview.LogicalViewEvaluator;
import io.carml.logicalview.ViewIteration;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.Source;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A {@link LogicalViewEvaluator} that compiles a {@link LogicalView} to a DuckDB SQL query via
 * {@link DuckDbViewCompiler}, executes it against a DuckDB JDBC {@link Connection}, and streams the
 * result rows as {@link ViewIteration}s.
 *
 * <p>Since DuckDB JDBC is blocking, the query execution is offloaded to a configurable Reactor
 * {@link Scheduler} (defaulting to {@link Schedulers#boundedElastic()}), preserving the reactive API
 * contract. On Java 21+, a virtual-thread-based scheduler can be injected for more efficient thread
 * utilization. From a downstream consumer's perspective, this evaluator produces a standard
 * {@code Flux<ViewIteration>} indistinguishable from a fully non-blocking evaluator.
 *
 * <p>The {@code sourceResolver} parameter from the {@link LogicalViewEvaluator} interface is not
 * used by this evaluator. DuckDB reads data files directly via functions such as
 * {@code read_json_auto}, {@code read_csv_auto}, and {@code read_parquet}.
 *
 * <p><strong>Thread safety:</strong> Each evaluator instance should have its own dedicated
 * {@link Connection}. When created by {@link DuckDbLogicalViewEvaluatorFactory}, each evaluator
 * receives a duplicated connection (via {@code DuckDBConnection.duplicate()}) that shares the same
 * underlying database but can be used independently. The evaluator closes its connection after
 * evaluation completes if it owns the connection.
 */
@Slf4j
public class DuckDbLogicalViewEvaluator implements LogicalViewEvaluator {

    static final String INDEX_KEY = "#";

    static final String INDEX_KEY_SUFFIX = ".#";

    /**
     * Maps DuckDB type strings to XSD datatype IRIs. This covers both {@code json_type()} return
     * values (e.g. {@code "BIGINT"}, {@code "DOUBLE"}) and {@code typeof()} return values for SQL
     * column types (e.g. {@code "INTEGER"}, {@code "FLOAT"}, {@code "DATE"}).
     *
     * <p>Types not in this map (e.g. {@code "VARCHAR"}, {@code "NULL"}, {@code "JSON"}) have no
     * natural RDF datatype. Parameterized types like {@code "DECIMAL(18,3)"} and
     * {@code "TIMESTAMP WITH TIME ZONE"} are handled via prefix matching in
     * {@link #resolveXsdType(String)}.
     */
    private static final Map<String, IRI> DUCKDB_TYPE_TO_XSD = Map.ofEntries(
            // JSON integer types (from json_type())
            Map.entry("BIGINT", XSD.INTEGER),
            Map.entry("UBIGINT", XSD.INTEGER),
            Map.entry("HUGEINT", XSD.INTEGER),
            Map.entry("UHUGEINT", XSD.INTEGER),
            // SQL integer types (from typeof())
            Map.entry("INTEGER", XSD.INTEGER),
            Map.entry("SMALLINT", XSD.INTEGER),
            Map.entry("TINYINT", XSD.INTEGER),
            Map.entry("UINTEGER", XSD.INTEGER),
            Map.entry("USMALLINT", XSD.INTEGER),
            Map.entry("UTINYINT", XSD.INTEGER),
            // Float/Double types
            Map.entry("DOUBLE", XSD.DOUBLE),
            Map.entry("FLOAT", XSD.DOUBLE),
            // Boolean
            Map.entry("BOOLEAN", XSD.BOOLEAN),
            // Date/Time types
            Map.entry("DATE", XSD.DATE),
            Map.entry("TIME", XSD.TIME),
            Map.entry("TIMESTAMP", XSD.DATETIME),
            // Binary
            Map.entry("BLOB", XSD.HEXBINARY));

    private final Connection connection;

    private final boolean ownsConnection;

    private final boolean useArrow;

    private final DuckDbSourceTableCache sourceTableCache;

    private final DuckDbDatabaseAttacher databaseAttacher;

    private final Semaphore concurrencyLimit;

    private final Scheduler scheduler;

    private volatile boolean permitAcquired;

    public DuckDbLogicalViewEvaluator(Connection connection) {
        this(connection, false, true, null, null, null, Schedulers.boundedElastic());
    }

    DuckDbLogicalViewEvaluator(
            Connection connection,
            boolean ownsConnection,
            DuckDbSourceTableCache sourceTableCache,
            Semaphore concurrencyLimit) {
        this(connection, ownsConnection, true, sourceTableCache, null, concurrencyLimit, Schedulers.boundedElastic());
    }

    DuckDbLogicalViewEvaluator(
            Connection connection,
            boolean ownsConnection,
            DuckDbSourceTableCache sourceTableCache,
            DuckDbDatabaseAttacher databaseAttacher,
            Semaphore concurrencyLimit,
            Scheduler scheduler) {
        this(connection, ownsConnection, true, sourceTableCache, databaseAttacher, concurrencyLimit, scheduler);
    }

    /** Package-private constructor for testing JDBC fallback path. */
    DuckDbLogicalViewEvaluator(Connection connection, boolean useArrow) {
        this(connection, false, useArrow, null, null, null, Schedulers.boundedElastic());
    }

    DuckDbLogicalViewEvaluator(
            Connection connection,
            boolean ownsConnection,
            boolean useArrow,
            DuckDbSourceTableCache sourceTableCache,
            Semaphore concurrencyLimit) {
        this(
                connection,
                ownsConnection,
                useArrow,
                sourceTableCache,
                null,
                concurrencyLimit,
                Schedulers.boundedElastic());
    }

    DuckDbLogicalViewEvaluator(
            Connection connection,
            boolean ownsConnection,
            boolean useArrow,
            DuckDbSourceTableCache sourceTableCache,
            DuckDbDatabaseAttacher databaseAttacher,
            Semaphore concurrencyLimit,
            Scheduler scheduler) {
        this.connection = connection;
        this.ownsConnection = ownsConnection;
        this.useArrow = useArrow;
        this.sourceTableCache = sourceTableCache;
        this.databaseAttacher = databaseAttacher;
        this.concurrencyLimit = concurrencyLimit;
        this.scheduler = scheduler;
    }

    @Override
    public Flux<ViewIteration> evaluate(
            LogicalView view, Function<Source, ResolvedSource<?>> sourceResolver, EvaluationContext context) {
        // Defer iterator construction so that errors from DuckDbViewCompiler.compile() and the
        // initial query execution are emitted as Flux error signals rather than synchronous
        // exceptions. Each evaluator has its own connection (via DuckDBConnection.duplicate()),
        // so no synchronization is needed; multiple evaluators can execute queries in parallel.
        //
        // The iterator pattern (Flux.using + Flux.fromIterable) provides natural backpressure:
        // {@link DuckDbViewIterator#next} is invoked once per downstream demand, and at most one
        // Arrow batch (~2048 rows) is buffered in memory at a time.
        return Flux.using(
                        () -> openIterator(view, context),
                        iter -> Flux.fromIterable(iter).cast(ViewIteration.class),
                        DuckDbViewIterator::close)
                .subscribeOn(scheduler);
    }

    @SuppressWarnings("java:S2077") // SQL is generated by jOOQ via DuckDbViewCompiler, not from user input
    private DuckDbViewIterator openIterator(LogicalView view, EvaluationContext context) {
        acquireConcurrencyPermit();
        ResultSet resultSet = null;
        java.sql.Statement statement = null;
        try {
            validateSourceFiles(view);
            validateSourceHandler(view);
            UnaryOperator<String> sourceTableResolver = sourceTableCache != null ? this::resolveSourceTable : null;
            var compiledView = DuckDbViewCompiler.compile(view, context, sourceTableResolver, databaseAttacher);
            LOG.debug("Executing DuckDB query for view [{}]", view.getResourceName());

            statement = connection.createStatement();
            resultSet = statement.executeQuery(compiledView.sql());

            var columnDescriptor = resolveColumns(resultSet);
            var referenceableKeys = DefaultLogicalViewEvaluator.collectReferenceableKeys(view);
            // Normalize column names: DuckDB lowercases unquoted SQL identifiers, but the view
            // definition preserves original case. Map DuckDB's lowercase names back to the
            // original case from referenceableKeys so field lookups match rml:reference values.
            columnDescriptor = normalizeColumnCase(columnDescriptor, referenceableKeys);

            var batchLoader = openBatchLoader(
                    view,
                    resultSet,
                    columnDescriptor,
                    compiledView,
                    referenceableKeys,
                    context.retainSourceEvaluation());

            return new DuckDbViewIterator(batchLoader, buildCleanup(resultSet, statement));
        } catch (SQLException e) {
            closeQuietly(resultSet, statement);
            releaseConcurrencyPermit();
            closeConnectionIfOwned();
            throw new DuckDbQueryException(
                    "Failed to execute DuckDB query for view [%s]".formatted(view.getResourceName()), e);
        } catch (RuntimeException e) {
            closeQuietly(resultSet, statement);
            releaseConcurrencyPermit();
            closeConnectionIfOwned();
            throw e;
        }
    }

    /**
     * Tries Arrow batch transfer first; falls back to JDBC row-by-row if Arrow is unavailable.
     * Arrow avoids the per-cell JNI overhead of {@code ResultSet.getObject()} by transferring data
     * in columnar batches via the C Data Interface.
     */
    private DuckDbViewIterator.BatchLoader openBatchLoader(
            LogicalView view,
            ResultSet resultSet,
            ColumnDescriptor columnDescriptor,
            CompiledView compiledView,
            Set<String> referenceableKeys,
            boolean retainSourceEvaluation) {
        if (useArrow && resultSet instanceof org.duckdb.DuckDBResultSet duckDbResultSet) {
            var arrowLoader = ArrowBatchEmitter.tryOpen(
                    duckDbResultSet, columnDescriptor, compiledView, referenceableKeys, retainSourceEvaluation);
            if (arrowLoader != null) {
                return arrowLoader;
            }
        }
        LOG.debug("Using JDBC row-by-row transfer for view [{}]", view.getResourceName());
        return new JdbcBatchLoader(
                resultSet, columnDescriptor, compiledView, referenceableKeys, retainSourceEvaluation);
    }

    /**
     * Builds the cleanup runnable wired into {@code Flux.using}'s resource consumer. Closes the
     * JDBC resources owned by {@link #openIterator}, releases the concurrency permit, and closes
     * the connection if owned. The {@link DuckDbViewIterator.BatchLoader}'s own {@code close()}
     * releases Arrow-specific resources.
     */
    private Runnable buildCleanup(ResultSet resultSet, java.sql.Statement statement) {
        return () -> {
            try {
                resultSet.close();
            } catch (SQLException ex) {
                LOG.debug("Error closing ResultSet", ex);
            }
            try {
                statement.close();
            } catch (SQLException ex) {
                LOG.debug("Error closing Statement", ex);
            }
            releaseConcurrencyPermit();
            closeConnectionIfOwned();
        };
    }

    private static void closeQuietly(ResultSet resultSet, java.sql.Statement statement) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException ignored) {
                // best-effort cleanup
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException ignored) {
                // best-effort cleanup
            }
        }
    }

    private String resolveSourceTable(String sourceSql) {
        var tableName = sourceTableCache.getOrCreateTable(sourceSql, connection);
        if (tableName == null) {
            return null;
        }
        return sourceTableCache.qualify(tableName);
    }

    private void acquireConcurrencyPermit() {
        if (concurrencyLimit != null) {
            try {
                concurrencyLimit.acquire();
                permitAcquired = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for DuckDB concurrency permit", e);
            }
        }
    }

    private void releaseConcurrencyPermit() {
        if (concurrencyLimit != null && permitAcquired) {
            permitAcquired = false;
            concurrencyLimit.release();
        }
    }

    private void closeConnectionIfOwned() {
        if (ownsConnection) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("Failed to close DuckDB connection", e);
            }
        }
    }

    /**
     * Validates that all file-based sources referenced by the view exist on the filesystem. Walks
     * the view tree recursively, resolving file paths against the DuckDB connection's
     * {@code file_search_path}. If any source file is missing, throws an
     * {@link IllegalArgumentException} that propagates as a Flux error signal.
     *
     * <p>This validation is necessary because DuckDB may silently return empty results for missing
     * files (e.g. when {@code read_text} + {@code json_extract} + {@code UNNEST} produces zero rows
     * instead of an error), which would cause the mapping to complete with an empty model instead of
     * signaling the error to the caller.
     */
    private void validateSourceFiles(LogicalView view) {
        var fileSearchPath = queryFileSearchPath();
        validateSourceFilesRecursive(view, fileSearchPath, new HashSet<>());
    }

    private void validateSourceFilesRecursive(LogicalView view, String fileSearchPath, Set<LogicalView> visited) {
        if (!visited.add(view)) {
            return;
        }
        var viewOn = view.getViewOn();
        if (viewOn instanceof LogicalSource logicalSource) {
            var source = logicalSource.getSource();
            if (DuckDbFileSourceUtils.isFileBasedSource(source)) {
                var filePath = DuckDbFileSourceUtils.resolveFilePath(source);
                validateFileExists(filePath, fileSearchPath, view);
            }
        } else if (viewOn instanceof LogicalView nestedView) {
            validateSourceFilesRecursive(nestedView, fileSearchPath, visited);
        }

        for (var join : view.getLeftJoins()) {
            validateSourceFilesRecursive(join.getParentLogicalView(), fileSearchPath, visited);
        }
        for (var join : view.getInnerJoins()) {
            validateSourceFilesRecursive(join.getParentLogicalView(), fileSearchPath, visited);
        }
    }

    private static void validateFileExists(String filePath, String fileSearchPath, LogicalView view) {
        // Skip validation for remote URLs — DuckDB can read remote sources directly
        // (http://, https://, s3://, gs://, az://, hf://, etc.)
        if (filePath.contains("://")) {
            return;
        }

        var path = Path.of(filePath);

        // Absolute paths can be checked directly
        if (path.isAbsolute()) {
            if (!path.toFile().exists()) {
                throw new IllegalArgumentException(
                        "Source file not found for view [%s]: %s".formatted(view.getResourceName(), filePath));
            }
            return;
        }

        // Relative paths: resolve against file_search_path
        if (fileSearchPath != null && !fileSearchPath.isBlank()) {
            var resolved = Path.of(fileSearchPath).resolve(filePath);
            if (!resolved.toFile().exists()) {
                throw new IllegalArgumentException("Source file not found for view [%s]: %s (resolved: %s)"
                        .formatted(view.getResourceName(), filePath, resolved));
            }
            return;
        }

        // No file_search_path: check relative to CWD
        if (!path.toFile().exists()) {
            throw new IllegalArgumentException(
                    "Source file not found for view [%s]: %s".formatted(view.getResourceName(), filePath));
        }
    }

    /**
     * Queries the DuckDB connection's {@code file_search_path} setting. Returns the configured path,
     * or {@code null} if not set or if the query fails.
     */
    private String queryFileSearchPath() {
        try (var statement = connection.createStatement();
                var resultSet = statement.executeQuery("SELECT current_setting('file_search_path')")) {
            if (resultSet.next()) {
                var result = resultSet.getString(1);
                return result != null && !result.isBlank() ? result : null;
            }
        } catch (SQLException ex) {
            LOG.debug("Could not query file_search_path setting", ex);
        }
        return null;
    }

    /**
     * Dispatches formulation-specific validation to the appropriate {@link DuckDbSourceHandler}.
     * For example, the CSV handler validates that field references match CSV column headers
     * case-sensitively.
     *
     * <p>Only applies to views backed by a {@link LogicalSource} with a reference formulation.
     * Views backed by other views or sources without a formulation are skipped.
     */
    private void validateSourceHandler(LogicalView view) {
        var viewOn = view.getViewOn();
        if (!(viewOn instanceof LogicalSource logicalSource)) {
            return;
        }

        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            return;
        }

        DuckDbSourceHandler.forFormulation(refFormulation.getAsResource())
                .ifPresent(handler -> handler.validate(view, connection));
    }

    private ColumnDescriptor resolveColumns(ResultSet resultSet) throws SQLException {
        var metadata = resultSet.getMetaData();
        var columnCount = metadata.getColumnCount();
        var valueColumns = new ArrayList<String>(columnCount);
        var typeColumns = new ArrayList<String>();
        var idxColumn = -1;
        var iterColumn = -1;
        String idxColumnName = null;
        String iterColumnName = null;

        for (var i = 1; i <= columnCount; i++) {
            var colName = metadata.getColumnLabel(i);
            if (DuckDbViewCompiler.INDEX_COLUMN.equals(colName)) {
                idxColumn = i;
                idxColumnName = colName;
            } else if (JsonPathSourceHandler.JSON_ITER_COLUMN.equals(colName)) {
                iterColumn = i;
                iterColumnName = colName;
            } else if (colName.endsWith(DuckDbSourceStrategy.TYPE_SUFFIX)) {
                typeColumns.add(colName);
            } else {
                valueColumns.add(colName);
            }
        }

        return new ColumnDescriptor(valueColumns, typeColumns, idxColumn, iterColumn, idxColumnName, iterColumnName);
    }

    private static ColumnDescriptor normalizeColumnCase(ColumnDescriptor columns, Set<String> referenceableKeys) {
        // Build a case-insensitive lookup from referenceable keys
        var lowerToOriginal = new HashMap<String, String>(referenceableKeys.size());
        for (var key : referenceableKeys) {
            lowerToOriginal.putIfAbsent(key.toLowerCase(Locale.ROOT), key);
        }

        List<String> normalized = columns.valueNames.stream()
                .map(name -> lowerToOriginal.getOrDefault(name.toLowerCase(Locale.ROOT), name))
                .toList();

        if (normalized.equals(columns.valueNames)) {
            return columns;
        }

        return new ColumnDescriptor(
                normalized,
                columns.typeNames,
                columns.idxColumn,
                columns.iterColumn,
                columns.idxColumnName,
                columns.iterColumnName);
    }

    /**
     * Pull-based JDBC batch loader for the row-by-row fallback path. Each {@code loadInto} call
     * reads up to {@link #CHUNK_SIZE} rows from the {@link ResultSet} into the buffer and returns
     * {@code true} if any rows were read, {@code false} when the result set is exhausted.
     */
    private record JdbcBatchLoader(
            ResultSet resultSet,
            ColumnDescriptor columns,
            CompiledView compiledView,
            Set<String> referenceableKeys,
            boolean retainSourceEvaluation)
            implements DuckDbViewIterator.BatchLoader {

        private static final int CHUNK_SIZE = 256;

        @Override
        public boolean loadInto(Deque<DuckDbViewIteration> buffer) {
            try {
                var loaded = 0;
                while (loaded < CHUNK_SIZE && resultSet.next()) {
                    var zeroBasedIndex = readZeroBasedIndex();
                    var values = readColumnValues(zeroBasedIndex);
                    validateNoNonScalarTypes();
                    var naturalDatatypes = resolveNaturalDatatypes(values);
                    var sourceEvaluation = resolveSourceEvaluation();
                    buffer.offer(DuckDbViewIteration.ofOwnedMaps(
                            zeroBasedIndex, values, naturalDatatypes, sourceEvaluation, referenceableKeys));
                    loaded++;
                }
                return loaded > 0;
            } catch (SQLException e) {
                throw new DuckDbQueryException("Failed to read row from DuckDB result set", e);
            }
        }

        @Override
        public void close() {
            // ResultSet/Statement are owned and closed by DuckDbViewIterator's cleanup.
        }

        private int readZeroBasedIndex() throws SQLException {
            // ROW_NUMBER() OVER() is 1-based; convert to 0-based for RML-LV "#" semantics
            var rawIndex = columns.idxColumn > 0 ? resultSet.getInt(columns.idxColumn) : 0;
            return rawIndex > 0 ? rawIndex - 1 : 0;
        }

        private LinkedHashMap<String, Object> readColumnValues(int zeroBasedIndex) throws SQLException {
            var values = new LinkedHashMap<String, Object>(columns.valueNames.size() + 1);
            values.put(INDEX_KEY, zeroBasedIndex);
            for (var colName : columns.valueNames) {
                values.put(colName, readColumnValue(colName));
            }
            return values;
        }

        private Object readColumnValue(String colName) throws SQLException {
            var rawValue = resultSet.getObject(colName);
            // Convert DuckDB LIST columns (java.sql.Array) to Java Lists, but only for
            // columns without a type companion. Columns with type companions are source
            // data fields where arrays indicate non-scalar values (which should be
            // rejected by validateNoNonScalarTypes). Columns without type companions are
            // aggregated join fields produced by list() where the LIST is intentional.
            if (rawValue instanceof java.sql.Array sqlArray
                    && !columns.typeNames.contains(colName + DuckDbSourceStrategy.TYPE_SUFFIX)) {
                var arrayData = sqlArray.getArray();
                if (arrayData instanceof Object[] objArray) {
                    return java.util.Arrays.asList(objArray);
                }
            }
            return rawValue;
        }

        /**
         * Creates source evaluation from raw JSON iterator column, if present and requested. This
         * enables gather map expressions (excluded from view fields) to be evaluated from the source
         * data using JSONPath at mapping time. Source evaluation is only retained when the context
         * requests it (e.g., implicit views with gather expressions), matching the reactive
         * evaluator's behavior. Without this guard, the source fallback in RdfTriplesMapper would
         * silently swallow invalid field references.
         */
        private ExpressionEvaluation resolveSourceEvaluation() throws SQLException {
            if (retainSourceEvaluation && columns.iterColumn > 0) {
                var rawJson = resultSet.getString(columns.iterColumn);
                if (rawJson != null) {
                    return new DuckDbJsonSourceEvaluation(rawJson);
                }
            }
            return null;
        }

        /**
         * Validates that no type companion column for a scalar (non-multi-valued) field contains a
         * non-scalar type value. Per the RML spec, a reference that resolves to an array or object
         * should produce an error, not a stringified value.
         *
         * <p>Type companions for multi-valued expression fields are excluded from validation. These
         * fields use UNNEST to expand array elements, and each element can legitimately be a JSON
         * object (OBJECT type) when the array contains objects.
         *
         * <p>The set of non-scalar type values is determined by the source strategy during
         * compilation and bundled into the {@link CompiledView}. For JSON sources, these are
         * {@code "ARRAY"} and {@code "OBJECT"}. For column-based sources (CSV, SQL), the set is
         * empty, so this validation is effectively a no-op.
         *
         * @throws IllegalArgumentException if any scalar field resolves to a non-scalar type
         */
        private void validateNoNonScalarTypes() throws SQLException {
            var nonScalarTypeValues = compiledView.nonScalarTypeValues();
            if (nonScalarTypeValues.isEmpty()) {
                return;
            }

            var multiValuedFieldNames = compiledView.multiValuedFieldNames();
            for (var typeCol : columns.typeNames) {
                var fieldName = typeCol.substring(0, typeCol.length() - DuckDbSourceStrategy.TYPE_SUFFIX.length());

                // Skip validation for multi-valued expression fields — their UNNEST elements can
                // legitimately be JSON objects or arrays.
                if (multiValuedFieldNames.contains(fieldName)) {
                    continue;
                }

                var duckDbType = resultSet.getString(typeCol);
                if (duckDbType != null && nonScalarTypeValues.contains(duckDbType)) {
                    throw new IllegalArgumentException("Reference '%s' resolves to a non-scalar value (type: %s). "
                                    .formatted(fieldName, duckDbType)
                            + "Per the RML spec, references must resolve to scalar values.");
                }
            }
        }

        /**
         * Resolves natural RDF datatypes from the type companion columns and ordinal columns in the
         * result set. Maps DuckDB {@code json_type()} string values to XSD IRIs and assigns
         * {@code xsd:integer} to all index and ordinal columns.
         */
        private Map<String, IRI> resolveNaturalDatatypes(Map<String, Object> values) throws SQLException {
            var naturalDatatypes = new LinkedHashMap<String, IRI>();

            // The "#" index key always has xsd:integer
            naturalDatatypes.put(INDEX_KEY, XSD.INTEGER);

            // All ordinal columns (.#) have xsd:integer
            for (var colName : values.keySet()) {
                if (colName.endsWith(INDEX_KEY_SUFFIX)) {
                    naturalDatatypes.put(colName, XSD.INTEGER);
                }
            }

            // Map type companion columns to XSD datatypes
            for (var typeCol : columns.typeNames) {
                var duckDbType = resultSet.getString(typeCol);
                if (duckDbType != null) {
                    var xsdType = resolveXsdType(duckDbType);
                    if (xsdType != null) {
                        // Strip the TYPE_SUFFIX to get the field name this type applies to
                        var fieldName =
                                typeCol.substring(0, typeCol.length() - DuckDbSourceStrategy.TYPE_SUFFIX.length());
                        naturalDatatypes.put(fieldName, xsdType);
                    }
                }
            }

            return naturalDatatypes;
        }
    }

    /**
     * Resolves a DuckDB type string to an XSD datatype IRI. First attempts an exact match in the
     * type map. If no exact match is found, falls back to prefix matching for parameterized types
     * such as {@code "DECIMAL(18,3)"} and {@code "TIMESTAMP WITH TIME ZONE"}.
     *
     * @param duckDbType the type string from DuckDB's {@code json_type()} or {@code typeof()}
     * @return the corresponding XSD IRI, or {@code null} if no mapping exists
     */
    static IRI resolveXsdType(String duckDbType) {
        var xsdType = DUCKDB_TYPE_TO_XSD.get(duckDbType);
        if (xsdType != null) {
            return xsdType;
        }
        if (duckDbType.startsWith("DECIMAL")) {
            return XSD.DECIMAL;
        }
        if (duckDbType.startsWith("TIMESTAMP")) {
            return XSD.DATETIME;
        }
        // TIME WITH TIME ZONE — must come after TIMESTAMP check to avoid false match
        if (duckDbType.startsWith("TIME")) {
            return XSD.TIME;
        }
        return null;
    }

    /**
     * Describes the column layout of a DuckDB query result set.
     *
     * <p>Carries both index-based fields (for JDBC access) and name-based fields (for Arrow access)
     * so that both transfer paths can use the same descriptor.
     *
     * @param valueNames ordered list of value column names (excludes type companions, idx, iter)
     * @param typeNames list of type companion column names (ending with {@code .__type})
     * @param idxColumn 1-based JDBC column index for the {@code __idx} column, or {@code -1} if
     *     absent
     * @param iterColumn 1-based JDBC column index for the {@code __iter} column, or {@code -1} if
     *     absent
     * @param idxColumnName column name for the index column, or {@code null} if absent
     * @param iterColumnName column name for the iter column, or {@code null} if absent
     */
    record ColumnDescriptor(
            List<String> valueNames,
            List<String> typeNames,
            int idxColumn,
            int iterColumn,
            String idxColumnName,
            String iterColumnName) {}
}
