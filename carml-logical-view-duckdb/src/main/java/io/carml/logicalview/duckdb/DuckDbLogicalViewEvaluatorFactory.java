package io.carml.logicalview.duckdb;

import com.google.auto.service.AutoService;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import io.carml.logicalview.FileBasePathConfigurable;
import io.carml.logicalview.LogicalViewEvaluatorFactory;
import io.carml.logicalview.MatchedLogicalViewEvaluator;
import io.carml.model.AbstractLogicalSource;
import io.carml.model.FilePath;
import io.carml.model.FileSource;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.vocab.Rdf;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;

/**
 * A {@link LogicalViewEvaluatorFactory} that matches {@link LogicalView} instances where all sources
 * in the view tree are DuckDB-compatible. When matched, returns a {@link DuckDbLogicalViewEvaluator}
 * with a strong match score.
 *
 * <p>DuckDB-compatible sources include:
 * <ul>
 *   <li>JSON (JsonPath reference formulation) -- via {@code read_json_auto}</li>
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

    private static final Set<Resource> COMPATIBLE_REF_FORMULATIONS = Set.of(
            Rdf.Ql.JsonPath,
            Rdf.Rml.JsonPath,
            Rdf.Ql.Csv,
            Rdf.Rml.Csv,
            Rdf.Ql.Rdb,
            Rdf.Rml.Rdb,
            Rdf.Rml.SQL2008Table,
            Rdf.Rml.SQL2008Query);

    private static final Set<Resource> FILE_BASED_REF_FORMULATIONS =
            Set.of(Rdf.Ql.JsonPath, Rdf.Rml.JsonPath, Rdf.Ql.Csv, Rdf.Rml.Csv);

    private static final MatchScore STRONG_MATCH =
            MatchScore.builder().strongMatch().build();

    private final Connection connection;

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
        var evaluator = new DuckDbLogicalViewEvaluator(connection);
        return Optional.of(MatchedLogicalViewEvaluator.of(STRONG_MATCH, evaluator));
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("Failed to close DuckDB connection", e);
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
     * source type. File-based reference formulations (CSV, JSON) require a {@link FilePath} or
     * {@link FileSource}; stream sources are not supported.
     */
    private boolean isLogicalSourceCompatible(LogicalSource logicalSource) {
        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            LOG.debug("LogicalSource has no reference formulation");
            return false;
        }

        var refIri = refFormulation.getAsResource();
        if (!COMPATIBLE_REF_FORMULATIONS.contains(refIri)) {
            return false;
        }

        if (FILE_BASED_REF_FORMULATIONS.contains(refIri) && !isFileBasedSource(logicalSource.getSource())) {
            LOG.debug("LogicalSource has file-based reference formulation but source is not file-based");
            return false;
        }

        return true;
    }

    private static boolean isFileBasedSource(io.carml.model.Source source) {
        return source instanceof FilePath || source instanceof FileSource;
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
