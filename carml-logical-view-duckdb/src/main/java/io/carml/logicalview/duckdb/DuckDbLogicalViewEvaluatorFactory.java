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
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

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
 * <p><strong>Thread safety:</strong> The factory itself is thread-safe for matching. However, the
 * produced {@link DuckDbLogicalViewEvaluator} instances share the underlying {@link Connection},
 * which is not thread-safe. See {@link DuckDbLogicalViewEvaluator} for details.
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

    private final DuckDbSourceTableCache sourceTableCache = new DuckDbSourceTableCache();

    /**
     * Creates a factory with an in-memory DuckDB connection. Used by {@link java.util.ServiceLoader}
     * for automatic discovery.
     */
    public DuckDbLogicalViewEvaluatorFactory() {
        this(createInMemoryConnection());
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
        applyFileSearchPath(this.connection, fileBasePath);
    }

    private DuckDbLogicalViewEvaluatorFactory(Connection connection, Path databasePath, Thread shutdownHook) {
        this.connection = connection;
        this.databasePath = databasePath;
        this.shutdownHook = shutdownHook;
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
        try {
            var tempDir = Files.createTempDirectory("carml-duckdb-");
            var dbPath = tempDir.resolve("carml.duckdb");
            LOG.info("Creating on-disk DuckDB database at: {}", dbPath);
            var conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath);

            var factoryHolder = new DuckDbLogicalViewEvaluatorFactory[1];
            // Lambda required: method reference would capture null (factoryHolder[0] is set after hook creation)
            @SuppressWarnings("java:S1612")
            var hook = new Thread(() -> factoryHolder[0].close(), "carml-duckdb-cleanup");
            Runtime.getRuntime().addShutdownHook(hook);

            var factory = new DuckDbLogicalViewEvaluatorFactory(conn, dbPath, hook);
            factoryHolder[0] = factory;
            return factory;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create on-disk DuckDB connection", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create temporary directory for DuckDB database", e);
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

    @Override
    public Optional<MatchedLogicalViewEvaluator> match(LogicalView view) {
        var visited = new HashSet<LogicalView>();
        if (!allSourcesCompatible(view, visited)) {
            LOG.debug("View [{}] contains incompatible sources for DuckDB evaluator", view.getResourceName());
            return Optional.empty();
        }

        LOG.debug("View [{}] matched for DuckDB evaluator", view.getResourceName());
        var evaluator = new DuckDbLogicalViewEvaluator(connection, sourceTableCache);
        return Optional.of(MatchedLogicalViewEvaluator.of(STRONG_MATCH, evaluator));
    }

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
}
