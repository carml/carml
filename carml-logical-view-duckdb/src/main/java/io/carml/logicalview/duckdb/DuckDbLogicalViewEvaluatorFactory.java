package io.carml.logicalview.duckdb;

import com.google.auto.service.AutoService;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import io.carml.logicalview.FileBasePathConfigurable;
import io.carml.logicalview.LogicalViewEvaluatorFactory;
import io.carml.logicalview.MatchedLogicalViewEvaluator;
import io.carml.model.AbstractLogicalSource;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBConnection;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A {@link LogicalViewEvaluatorFactory} that matches {@link LogicalView} instances where all sources
 * in the view tree are DuckDB-compatible. When matched, returns a {@link DuckDbLogicalViewEvaluator}
 * with a strong match score.
 *
 * <p>DuckDB-compatible sources include:
 * <ul>
 *   <li>JSON (JsonPath reference formulation) -- via {@code read_text} + {@code json_extract} + {@code unnest}
 *       for iterators, {@code read_json_auto} for non-iterator sources</li>
 *   <li>CSV -- via {@code read_csv_auto}</li>
 *   <li>SQL databases (RDB, SQL2008Table, SQL2008Query) -- via database scanner extensions</li>
 * </ul>
 *
 * <p>Incompatible sources that cause the factory to decline:
 * <ul>
 *   <li>XPath/XML -- DuckDB has no native XML support</li>
 *   <li>SPARQL endpoints -- not a file or database source</li>
 *   <li>Stream sources (carml:Stream) -- data not available as a seekable file</li>
 *   <li>Unknown reference formulations</li>
 * </ul>
 *
 * <p>The factory walks the entire view tree recursively, including:
 * <ul>
 *   <li>The root view's {@code viewOn} source</li>
 *   <li>Parent views referenced by {@link LogicalViewJoin} (left joins and inner joins)</li>
 * </ul>
 *
 * <p>If any source in the tree is incompatible, the factory returns empty, allowing the
 * {@link io.carml.logicalview.DefaultLogicalViewEvaluatorFactory} (reactive fallback) to handle the
 * view instead.
 *
 * <p><strong>Thread safety:</strong> The factory itself is thread-safe for matching. Each produced
 * {@link DuckDbLogicalViewEvaluator} receives its own duplicated {@link Connection} (via
 * {@link DuckDBConnection#duplicate()}), enabling parallel evaluation of multiple views. The
 * duplicated connections share the same underlying DuckDB database, so regular tables created by
 * the shared {@link DuckDbSourceTableCache} are visible to all evaluators.
 */
@Slf4j
@AutoService(LogicalViewEvaluatorFactory.class)
public class DuckDbLogicalViewEvaluatorFactory
        implements LogicalViewEvaluatorFactory, FileBasePathConfigurable, AutoCloseable {

    private static final MatchScore STRONG_MATCH =
            MatchScore.builder().strongMatch().build();

    private final Connection connection;

    private final Path databasePath;

    private final Thread shutdownHook;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final DuckDbSourceTableCache sourceTableCache;

    private final Scheduler scheduler;

    private final boolean ownsScheduler;

    /**
     * The native catalog and schema of the DuckDB database, determined at construction time before
     * any {@code USE} commands are applied. Used to detect whether {@link #currentCatalog} and
     * {@link #currentSchema} have been changed by external code.
     */
    private final String nativeCatalog;

    private final String nativeSchema;

    @SuppressWarnings("java:S3077") // Path is immutable, so volatile is sufficient
    private volatile Path fileBasePath;

    private volatile String currentCatalog;

    private volatile String currentSchema;

    /**
     * Limits the number of concurrent DuckDB query executions to the number of available CPU cores.
     * Each duplicated connection runs a heavy vectorized SQL query; exceeding the core count causes
     * CPU contention that degrades throughput. The semaphore is acquired by evaluators before query
     * execution and released on completion (via {@code doFinally}).
     */
    private final Semaphore concurrencyLimit =
            new Semaphore(Runtime.getRuntime().availableProcessors());

    /**
     * Creates a factory with an in-memory DuckDB connection. Used by {@link java.util.ServiceLoader}
     * for automatic discovery.
     */
    public DuckDbLogicalViewEvaluatorFactory() {
        this(createInMemoryConnection());
    }

    /**
     * Creates a factory with an in-memory DuckDB connection and a custom scheduler for offloading
     * blocking JDBC calls. This enables callers to supply a virtual-thread-based scheduler (e.g. on
     * Java 21+) for more efficient thread utilization.
     *
     * <p>The factory takes ownership of the scheduler and will {@link Scheduler#dispose() dispose}
     * it when {@link #close()} is called.
     *
     * @param scheduler the Reactor {@link Scheduler} for offloading blocking query execution
     */
    public DuckDbLogicalViewEvaluatorFactory(Scheduler scheduler) {
        this(createInMemoryConnection(), scheduler);
    }

    /**
     * Creates a factory with the given DuckDB JDBC connection.
     *
     * @param connection the DuckDB JDBC connection to use for query execution
     */
    public DuckDbLogicalViewEvaluatorFactory(Connection connection) {
        this.connection = connection;
        this.databasePath = null;
        this.shutdownHook = null;
        this.sourceTableCache = new DuckDbSourceTableCache(connection);
        var nativeCatalogSchema = resolveNativeCatalogSchema(connection);
        this.nativeCatalog = nativeCatalogSchema[0];
        this.nativeSchema = nativeCatalogSchema[1];
        this.scheduler = Schedulers.boundedElastic();
        this.ownsScheduler = false;
    }

    /**
     * Creates a factory with the given DuckDB JDBC connection and a custom scheduler for offloading
     * blocking JDBC calls. This enables callers to supply a virtual-thread-based scheduler (e.g. on
     * Java 21+) for more efficient thread utilization.
     *
     * <p>The factory takes ownership of the scheduler and will {@link Scheduler#dispose() dispose}
     * it when {@link #close()} is called.
     *
     * @param connection the DuckDB JDBC connection to use for query execution
     * @param scheduler the Reactor {@link Scheduler} for offloading blocking query execution
     */
    public DuckDbLogicalViewEvaluatorFactory(Connection connection, Scheduler scheduler) {
        this.connection = connection;
        this.databasePath = null;
        this.shutdownHook = null;
        this.sourceTableCache = new DuckDbSourceTableCache(connection);
        var nativeCatalogSchema = resolveNativeCatalogSchema(connection);
        this.nativeCatalog = nativeCatalogSchema[0];
        this.nativeSchema = nativeCatalogSchema[1];
        this.scheduler = scheduler;
        this.ownsScheduler = true;
    }

    /**
     * Creates a factory with the given DuckDB JDBC connection and file base path. Equivalent to
     * creating the factory with the connection and then calling {@link #setFileBasePath(Path)}.
     *
     * @param connection the DuckDB JDBC connection to use for query execution
     * @param fileBasePath the base path for resolving relative file references
     */
    public DuckDbLogicalViewEvaluatorFactory(Connection connection, Path fileBasePath) {
        this(connection);
        this.fileBasePath = fileBasePath;
        applyFileSearchPath(this.connection, fileBasePath);
    }

    /**
     * Creates a factory with the given DuckDB JDBC connection, file base path, and custom scheduler.
     *
     * <p>The factory takes ownership of the scheduler and will {@link Scheduler#dispose() dispose}
     * it when {@link #close()} is called.
     *
     * @param connection the DuckDB JDBC connection to use for query execution
     * @param fileBasePath the base path for resolving relative file references
     * @param scheduler the Reactor {@link Scheduler} for offloading blocking query execution
     */
    public DuckDbLogicalViewEvaluatorFactory(Connection connection, Path fileBasePath, Scheduler scheduler) {
        this(connection, scheduler);
        this.fileBasePath = fileBasePath;
        applyFileSearchPath(this.connection, fileBasePath);
    }

    private DuckDbLogicalViewEvaluatorFactory(
            Connection connection, Path databasePath, Thread shutdownHook, Scheduler scheduler, boolean ownsScheduler) {
        this.connection = connection;
        this.databasePath = databasePath;
        this.shutdownHook = shutdownHook;
        this.sourceTableCache = new DuckDbSourceTableCache(connection);
        var nativeCatalogSchema = resolveNativeCatalogSchema(connection);
        this.nativeCatalog = nativeCatalogSchema[0];
        this.nativeSchema = nativeCatalogSchema[1];
        this.scheduler = scheduler;
        this.ownsScheduler = ownsScheduler;
    }

    /**
     * Creates a factory backed by an on-disk DuckDB database in a temporary directory. This enables
     * processing of larger-than-memory datasets by allowing DuckDB to spill to disk.
     *
     * <p>The temporary database files are cleaned up when the factory is {@link #close() closed} or
     * when the JVM shuts down (whichever comes first).
     *
     * @return a new factory with an on-disk DuckDB connection
     */
    public static DuckDbLogicalViewEvaluatorFactory createOnDisk() {
        return createOnDisk(Schedulers.boundedElastic(), false, null);
    }

    /**
     * Creates a factory backed by an on-disk DuckDB database in a temporary directory, using the
     * given scheduler for offloading blocking JDBC calls. This enables callers to supply a
     * virtual-thread-based scheduler (e.g. on Java 21+) for more efficient thread utilization.
     *
     * <p>The factory takes ownership of the scheduler and will {@link Scheduler#dispose() dispose}
     * it when {@link #close()} is called.
     *
     * <p>The temporary database files are cleaned up when the factory is {@link #close() closed} or
     * when the JVM shuts down (whichever comes first).
     *
     * @param scheduler the Reactor {@link Scheduler} for offloading blocking query execution
     * @return a new factory with an on-disk DuckDB connection
     */
    public static DuckDbLogicalViewEvaluatorFactory createOnDisk(Scheduler scheduler) {
        return createOnDisk(scheduler, true, null);
    }

    /**
     * Creates an on-disk factory with a custom scheduler and explicit memory limit.
     *
     * @param scheduler the Reactor {@link Scheduler} for offloading blocking query execution
     * @param memoryLimit explicit DuckDB memory limit (e.g. {@code "4GB"}), or {@code null} for auto-tuning
     * @return a new factory with an on-disk DuckDB connection
     */
    public static DuckDbLogicalViewEvaluatorFactory createOnDisk(Scheduler scheduler, String memoryLimit) {
        return createOnDisk(scheduler, true, memoryLimit);
    }

    private static DuckDbLogicalViewEvaluatorFactory createOnDisk(
            Scheduler scheduler, boolean ownsScheduler, String memoryLimit) {
        try {
            var tempDir = Files.createTempDirectory("carml-duckdb-");
            var dbPath = tempDir.resolve("carml.duckdb");
            LOG.info("Creating on-disk DuckDB database at: {}", dbPath);
            var conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);

            applyOnDiskSettings(conn, dbPath, memoryLimit);

            var factoryHolder = new DuckDbLogicalViewEvaluatorFactory[1];
            // Lambda required: method reference would capture null (factoryHolder[0] is set after hook creation).
            // Null check guards against the tiny window between addShutdownHook and factoryHolder assignment.
            @SuppressWarnings("java:S1612")
            var hook = new Thread(
                    () -> {
                        if (factoryHolder[0] != null) {
                            factoryHolder[0].close();
                        }
                    },
                    "carml-duckdb-cleanup");
            Runtime.getRuntime().addShutdownHook(hook);

            var factory = new DuckDbLogicalViewEvaluatorFactory(conn, dbPath, hook, scheduler, ownsScheduler);
            factoryHolder[0] = factory;
            return factory;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create on-disk DuckDB connection", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create temporary directory for DuckDB database", e);
        }
    }

    /** Validates DuckDB memory limit format (e.g. "4GB", "512MB", "2.5GiB"). */
    private static final Pattern MEMORY_LIMIT_PATTERN =
            Pattern.compile("^\\d+(\\.\\d+)?\\s*(B|KB|KiB|MB|MiB|GB|GiB|TB|TiB)$", Pattern.CASE_INSENSITIVE);

    /**
     * Estimated total memory per DuckDB thread including both buffer-managed and non-buffer-managed
     * allocations (CSV reader ~32MB, vectors, hash table partitions). Used to determine thread count
     * from total available memory.
     */
    private static final long ESTIMATED_MEMORY_PER_THREAD_BYTES = 250L * 1024 * 1024;

    /**
     * Estimated non-buffer-managed memory per thread (vectors, CSV reader buffers, hash table build
     * structures). Subtracted from total budget to derive the buffer pool size.
     */
    private static final long NON_BUFFER_OVERHEAD_PER_THREAD_BYTES = 150L * 1024 * 1024;

    /**
     * Configures DuckDB for on-disk mode with resource-aware tuning.
     *
     * <p>DuckDB's {@code memory_limit} only controls the buffer manager pool — vectors, hash
     * tables, CSV reader buffers, and query results allocate <em>outside</em> the buffer manager.
     * Therefore, thread count is the primary lever for total memory control: each thread
     * independently allocates ~150 MB of non-buffer memory. The tuning formula:
     *
     * <pre>
     *   total_available = system_memory - JVM_heap - 512MB
     *   threads         = max(1, total_available / 250MB)
     *   memory_limit    = total_available - threads * 150MB
     * </pre>
     *
     * <p>When {@code memoryLimitOverride} is specified, only {@code memory_limit} is set to the
     * user's value. Thread count is still auto-tuned based on total available memory.
     *
     * <p>The {@code temp_directory} is set to {@code /duckdb-tmp} if that directory exists
     * (e.g., in Docker), otherwise to a subdirectory next to the database file.
     *
     * @param memoryLimitOverride explicit DuckDB memory limit (e.g. {@code "4GB"}), or {@code null}
     *     to auto-tune based on available system memory
     */
    @SuppressWarnings({
        "java:S2077", // SET commands use validated/computed values, not arbitrary user input
        "java:S3649" // temp_directory is a controlled Path; memoryLimitOverride is regex-validated
    })
    private static void applyOnDiskSettings(Connection conn, Path databasePath, String memoryLimitOverride) {
        // Validate memoryLimitOverride before interpolating into SQL
        if (memoryLimitOverride != null
                && !memoryLimitOverride.isBlank()
                && !MEMORY_LIMIT_PATTERN.matcher(memoryLimitOverride).matches()) {
            throw new IllegalArgumentException(
                    "Invalid memory limit format: '%s'. Expected: <number><unit> (e.g. '4GB', '512MB')"
                            .formatted(memoryLimitOverride));
        }

        try (var stmt = conn.createStatement()) {
            var totalAvailable = resolveAvailableMemory();

            // 1. Set thread count based on total available memory (~250MB per thread)
            var threads = Math.max(1, totalAvailable / ESTIMATED_MEMORY_PER_THREAD_BYTES);
            var availableProcessors = (long) Runtime.getRuntime().availableProcessors();
            threads = Math.min(threads, availableProcessors);
            if (threads < availableProcessors) {
                stmt.execute("SET threads = %d".formatted(threads));
            }
            LOG.info(
                    "Set DuckDB threads to {} ({}MB available / 250MB per thread)",
                    threads,
                    totalAvailable / (1024 * 1024));

            // 2. Set memory_limit (buffer pool size)
            if (memoryLimitOverride != null && !memoryLimitOverride.isBlank()) {
                stmt.execute("SET memory_limit = '%s'".formatted(memoryLimitOverride));
                LOG.info("Set DuckDB memory_limit to {} (user-specified)", memoryLimitOverride);
            } else {
                // Buffer pool = total available minus non-buffer overhead per thread
                var bufferPoolBytes =
                        Math.max(50L * 1024 * 1024, totalAvailable - threads * NON_BUFFER_OVERHEAD_PER_THREAD_BYTES);
                stmt.execute("SET memory_limit = '%dB'".formatted(bufferPoolBytes));
                LOG.info(
                        "Set DuckDB memory_limit to {}MB ({}MB available - {} threads * 150MB non-buffer overhead)",
                        bufferPoolBytes / (1024 * 1024),
                        totalAvailable / (1024 * 1024),
                        threads);
            }

            // 3. Disable insertion order preservation to reduce peak memory during table creation
            stmt.execute("SET preserve_insertion_order = false");

            // 4. Set temp_directory for spilling intermediate query results
            var tempDir = Path.of("/duckdb-tmp");
            if (!Files.isDirectory(tempDir)) {
                tempDir = databasePath.resolveSibling("duckdb-tmp");
                Files.createDirectories(tempDir);
            }
            var escapedTempDir = tempDir.toString().replace("'", "''");
            stmt.execute("SET temp_directory = '%s'".formatted(escapedTempDir));
            LOG.info("Set DuckDB temp_directory to {}", tempDir);
        } catch (SQLException e) {
            LOG.warn("Failed to configure DuckDB on-disk settings: {}", e.getMessage());
        } catch (IOException e) {
            LOG.warn("Failed to create DuckDB temp directory: {}", e.getMessage());
        }
    }

    /**
     * Resolves total memory available for DuckDB by querying system memory via
     * {@code OperatingSystemMXBean}. Falls back to JVM max heap if the platform-specific
     * MXBean is unavailable (e.g., on non-HotSpot JVMs).
     *
     * <p>Note: {@code Runtime.getRuntime().maxMemory()} returns {@code Long.MAX_VALUE} when the
     * JVM heap is uncapped ({@code -Xmx} not set). The {@code Math.max} floor in the caller
     * handles this by falling back to {@link #ESTIMATED_MEMORY_PER_THREAD_BYTES}.
     */
    private static long resolveAvailableMemory() {
        var jvmMaxHeap = Runtime.getRuntime().maxMemory();
        var overheadBuffer = 512L * 1024 * 1024;

        try {
            var osBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            var systemMemory = osBean.getTotalMemorySize();
            return Math.max(ESTIMATED_MEMORY_PER_THREAD_BYTES, systemMemory - jvmMaxHeap - overheadBuffer);
        } catch (ClassCastException e) {
            // Non-HotSpot JVM (e.g., OpenJ9) — fall back to JVM heap as proxy
            LOG.warn("OperatingSystemMXBean not available, using JVM max heap as memory estimate");
            return Math.max(ESTIMATED_MEMORY_PER_THREAD_BYTES, jvmMaxHeap - overheadBuffer);
        }
    }

    /**
     * Returns the path to the on-disk database file, or {@code null} for in-memory mode.
     */
    Path getDatabasePath() {
        return databasePath;
    }

    @Override
    public void setFileBasePath(Path basePath) {
        this.fileBasePath = basePath;
        applyFileSearchPath(connection, basePath);
    }

    // SET does not support parameterized queries, so single quotes must be escaped manually.
    private static void applyFileSearchPath(Connection conn, Path basePath) {
        try (var statement = conn.createStatement()) {
            var absolutePath = basePath.toAbsolutePath().toString();
            var escapedPath = absolutePath.replace("'", "''");
            LOG.debug("Setting DuckDB file_search_path to: {}", absolutePath);
            statement.execute("SET file_search_path = '%s'".formatted(escapedPath));
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to set DuckDB file_search_path to: %s".formatted(basePath), e);
        }
    }

    /**
     * Propagates connection-level settings to a duplicated connection.
     * {@code DuckDBConnection.duplicate()} shares the same database but starts with default
     * connection settings. This method copies:
     * <ul>
     *   <li>{@code file_search_path} — for resolving relative file references</li>
     *   <li>{@code current_catalog} and {@code current_schema} — for SQL sources using
     *       {@code USE <catalog>.<schema>} (e.g., attached MySQL/PostgreSQL databases)</li>
     * </ul>
     *
     * <p>Catalog and schema are cached from the base connection when first set (via
     * {@code USE} commands from test infrastructure or scanner setup) to avoid querying the
     * base connection from concurrent threads.
     */
    private void propagateConnectionSettings(Connection target) throws SQLException {
        var storedBasePath = fileBasePath;
        if (storedBasePath != null) {
            applyFileSearchPath(target, storedBasePath);
        }

        var catalog = currentCatalog;
        var schema = currentSchema;
        if (catalog != null && schema != null && !(nativeCatalog.equals(catalog) && nativeSchema.equals(schema))) {
            try (var stmt = target.createStatement()) {
                stmt.execute(
                        "USE \"%s\".\"%s\"".formatted(catalog.replace("\"", "\"\""), schema.replace("\"", "\"\"")));
            }
        }
    }

    /**
     * Caches the current catalog and schema from the base connection. Called once before evaluations
     * start (from {@link #match}) to avoid querying the base connection from concurrent threads.
     */
    private synchronized void cacheCurrentCatalogAndSchema() {
        if (currentCatalog != null) {
            return;
        }
        try (var stmt = connection.createStatement();
                var rs = stmt.executeQuery("SELECT current_catalog(), current_schema()")) {
            if (rs.next()) {
                currentCatalog = rs.getString(1);
                currentSchema = rs.getString(2);
            }
        } catch (SQLException e) {
            LOG.debug("Could not read current catalog/schema", e);
        }
    }

    @Override
    public Optional<MatchedLogicalViewEvaluator> match(LogicalView view) {
        var visited = new HashSet<LogicalView>();
        if (!allSourcesCompatible(view, visited)) {
            LOG.debug("View [{}] contains incompatible sources for DuckDB evaluator", view.getResourceName());
            return Optional.empty();
        }

        LOG.debug("View [{}] matched for DuckDB evaluator", view.getResourceName());

        // Try to create a per-evaluator duplicated connection so multiple views can be evaluated
        // in parallel. DuckDBConnection.duplicate() creates a new connection that shares the same
        // underlying database, so regular tables (source cache) are visible to all evaluators.
        // If the connection is not a DuckDBConnection (e.g. in tests with mocks), fall back to
        // using the base connection directly (existing single-threaded behavior).
        // Cache catalog/schema from the base connection before any duplication. This avoids
        // querying the base connection from concurrent threads.
        cacheCurrentCatalogAndSchema();

        Connection evalConnection = connection;
        boolean ownsConnection = false;
        if (connection instanceof DuckDBConnection duckDbConnection) {
            try {
                evalConnection = duckDbConnection.duplicate();
                ownsConnection = true;
                propagateConnectionSettings(evalConnection);
            } catch (Exception e) {
                LOG.warn("Failed to duplicate/configure DuckDB connection, falling back to shared connection", e);
                if (ownsConnection) {
                    try {
                        evalConnection.close();
                    } catch (SQLException ex) {
                        LOG.debug("Failed to close leaked duplicated connection", ex);
                    }
                }
                evalConnection = connection;
                ownsConnection = false;
            }
        }

        var evaluator = new DuckDbLogicalViewEvaluator(
                evalConnection, ownsConnection, sourceTableCache, concurrencyLimit, scheduler);
        return Optional.of(MatchedLogicalViewEvaluator.of(STRONG_MATCH, evaluator));
    }

    /**
     * Closes the factory and its base DuckDB connection. The caller must ensure that all
     * evaluations produced by {@link #match} have completed before calling this method.
     * In-flight evaluators use duplicated connections that close independently, but they
     * reference shared source cache tables that are dropped here.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        try {
            if (connection != null && !connection.isClosed()) {
                sourceTableCache.clear(connection);
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("Failed to close DuckDB connection", e);
        }

        if (ownsScheduler) {
            scheduler.dispose();
        }

        if (databasePath != null) {
            deleteDatabaseFiles();
        }

        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // JVM is already shutting down — hook removal not possible, which is fine
            }
        }
    }

    private void deleteDatabaseFiles() {
        var parentDir = databasePath.getParent();
        try (var entries = Files.walk(parentDir)) {
            entries.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    LOG.warn("Failed to delete DuckDB file: {}", path, e);
                }
            });
        } catch (IOException e) {
            LOG.warn("Failed to clean up temporary DuckDB directory: {}", parentDir, e);
        }
    }

    /**
     * Recursively checks whether all sources in the view tree are DuckDB-compatible. Tracks visited
     * views to avoid infinite recursion in cyclic view graphs.
     */
    private boolean allSourcesCompatible(LogicalView view, Set<LogicalView> visited) {
        if (!visited.add(view)) {
            return true;
        }

        if (!isViewOnCompatible(view.getViewOn(), visited)) {
            return false;
        }

        if (!allJoinParentViewsCompatible(view.getLeftJoins(), visited)) {
            return false;
        }

        return allJoinParentViewsCompatible(view.getInnerJoins(), visited);
    }

    /**
     * Checks whether the {@code viewOn} target is compatible. If it is a {@link LogicalSource}, its
     * reference formulation is checked against the set of compatible formulations. If it is a nested
     * {@link LogicalView}, its entire view tree is checked recursively.
     */
    private boolean isViewOnCompatible(AbstractLogicalSource viewOn, Set<LogicalView> visited) {
        if (viewOn == null) {
            LOG.debug("viewOn is null");
            return false;
        }

        if (viewOn instanceof LogicalSource logicalSource) {
            return isLogicalSourceCompatible(logicalSource);
        }

        if (viewOn instanceof LogicalView nestedView) {
            return allSourcesCompatible(nestedView, visited);
        }

        LOG.debug("Unknown viewOn type: {}", viewOn.getClass().getName());
        return false;
    }

    /**
     * Checks whether a {@link LogicalSource} has a DuckDB-compatible reference formulation and
     * source type. Delegates formulation-specific compatibility checks to
     * {@link DuckDbSourceHandler} implementations.
     */
    private static boolean isLogicalSourceCompatible(LogicalSource logicalSource) {
        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            LOG.debug("LogicalSource has no reference formulation");
            return false;
        }

        var refIri = refFormulation.getAsResource();
        return DuckDbSourceHandler.forFormulation(refIri)
                .map(handler -> handler.isCompatible(logicalSource))
                .orElse(false);
    }

    /**
     * Checks whether all parent views referenced by the given joins are compatible.
     */
    private boolean allJoinParentViewsCompatible(Set<LogicalViewJoin> joins, Set<LogicalView> visited) {
        return joins.stream().allMatch(join -> allSourcesCompatible(join.getParentLogicalView(), visited));
    }

    private static Connection createInMemoryConnection() {
        try {
            return DriverManager.getConnection("jdbc:duckdb:");
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to create in-memory DuckDB connection. Ensure DuckDB JDBC is on the classpath.", e);
        }
    }

    /**
     * Queries the connection for its current catalog and schema. Must be called before any
     * {@code USE <catalog>.<schema>} commands are applied, so the result reflects the native
     * catalog/schema of the DuckDB database.
     *
     * @return a two-element array: {@code [catalog, schema]}
     */
    private static String[] resolveNativeCatalogSchema(Connection conn) {
        try (var stmt = conn.createStatement();
                var rs = stmt.executeQuery("SELECT current_catalog(), current_schema()")) {
            if (rs.next()) {
                return new String[] {rs.getString(1), rs.getString(2)};
            }
        } catch (SQLException e) {
            LOG.warn("Could not determine native catalog/schema, defaulting to memory.main", e);
        }
        return new String[] {"memory", "main"};
    }
}
