package io.carml.logicalview.sql;

import com.google.auto.service.AutoService;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import io.carml.logicalview.LogicalViewEvaluatorFactory;
import io.carml.logicalview.MatchedLogicalViewEvaluator;
import io.carml.model.AbstractLogicalSource;
import io.carml.model.DatabaseSource;
import io.carml.model.LogicalSource;
import io.carml.model.LogicalView;
import io.carml.model.LogicalViewJoin;
import io.carml.vocab.Rdf;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;

/**
 * A {@link LogicalViewEvaluatorFactory} that matches {@link LogicalView} instances where all sources
 * in the view tree are SQL database sources from the <strong>same database</strong>. When matched,
 * returns a {@link ReactiveSqlLogicalViewEvaluator} that executes queries directly against the source
 * database via Vert.x, bypassing DuckDB.
 *
 * <p>This factory scores higher than the DuckDB factory for same-database views
 * ({@code strongMatch + strongMatch = 4} vs. DuckDB's {@code strongMatch = 2}), ensuring the reactive
 * SQL evaluator is preferred when applicable.
 *
 * <p>The factory creates one Vert.x connection pool per unique {@link DatabaseSourceIdentity},
 * cached and reused across evaluations. The factory and its pools are closed via
 * {@link #close()}.
 *
 * <p>Matching criteria:
 * <ul>
 *   <li>Every {@link LogicalSource} in the view tree has a SQL reference formulation
 *       (Rdb, SQL2008Table, SQL2008Query)</li>
 *   <li>Every source is a {@link DatabaseSource}</li>
 *   <li>All {@link DatabaseSource} instances share the same {@link DatabaseSourceIdentity}</li>
 *   <li>A {@link SqlClientProvider} exists for the database type</li>
 * </ul>
 */
@Slf4j
@AutoService(LogicalViewEvaluatorFactory.class)
public class ReactiveSqlLogicalViewEvaluatorFactory implements LogicalViewEvaluatorFactory, AutoCloseable {

    private static final Set<Resource> SQL_FORMULATIONS =
            Set.of(Rdf.Ql.Rdb, Rdf.Rml.Rdb, Rdf.Rml.SQL2008Table, Rdf.Rml.SQL2008Query);

    /**
     * Score 4: two strong matches. Beats DuckDB's single strong match (score 2) for same-database
     * SQL views.
     */
    private static final MatchScore REACTIVE_SQL_MATCH =
            MatchScore.builder().strongMatch().strongMatch().build();

    private final List<SqlClientProvider> providers;

    private final Vertx vertx;

    private final int cursorBatchSize;

    private final Map<DatabaseSourceIdentity, Pool> poolCache = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a factory with auto-discovered {@link SqlClientProvider}s. Used by
     * {@link ServiceLoader} for automatic discovery.
     */
    public ReactiveSqlLogicalViewEvaluatorFactory() {
        this.providers = ServiceLoader.load(SqlClientProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .toList();
        this.vertx = Vertx.vertx();
        this.cursorBatchSize = ReactiveSqlLogicalViewEvaluator.DEFAULT_CURSOR_BATCH_SIZE;
    }

    /**
     * Creates a factory with explicit providers and Vert.x instance.
     *
     * @param providers the SQL client providers to use for database matching
     * @param vertx the Vert.x instance for connection pool creation
     */
    public ReactiveSqlLogicalViewEvaluatorFactory(List<SqlClientProvider> providers, Vertx vertx) {
        this(providers, vertx, ReactiveSqlLogicalViewEvaluator.DEFAULT_CURSOR_BATCH_SIZE);
    }

    /**
     * Creates a factory with explicit providers, Vert.x instance, and cursor batch size.
     *
     * @param providers the SQL client providers to use for database matching
     * @param vertx the Vert.x instance for connection pool creation
     * @param cursorBatchSize the number of rows to fetch per cursor batch
     */
    public ReactiveSqlLogicalViewEvaluatorFactory(List<SqlClientProvider> providers, Vertx vertx, int cursorBatchSize) {
        this.providers = providers;
        this.vertx = vertx;
        this.cursorBatchSize = cursorBatchSize;
    }

    @Override
    public Optional<MatchedLogicalViewEvaluator> match(LogicalView view) {
        if (providers.isEmpty()) {
            return Optional.empty();
        }

        var visited = new HashSet<LogicalView>();
        var databaseSources = new HashSet<DatabaseSource>();

        if (!allSourcesSameSqlDatabase(view, visited, databaseSources)) {
            return Optional.empty();
        }

        if (databaseSources.isEmpty()) {
            return Optional.empty();
        }

        // All sources point to the same database — pick one to identify the target
        var representativeSource = databaseSources.iterator().next();
        var provider = findProvider(representativeSource);
        if (provider.isEmpty()) {
            LOG.debug("No SqlClientProvider found for driver [{}]", representativeSource.getJdbcDriver());
            return Optional.empty();
        }

        var identity = DatabaseSourceIdentity.of(representativeSource);
        var pool = poolCache.computeIfAbsent(identity, id -> provider.get().createPool(vertx, representativeSource));
        var evaluator =
                new ReactiveSqlLogicalViewEvaluator(pool, provider.get().dialect(), provider.get(), cursorBatchSize);

        LOG.debug(
                "View [{}] matched for reactive SQL evaluator (database: {})",
                view.getResourceName(),
                identity.jdbcDsn());
        return Optional.of(MatchedLogicalViewEvaluator.of(REACTIVE_SQL_MATCH, evaluator));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        for (var pool : poolCache.values()) {
            try {
                pool.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Vert.x SQL pool", e);
            }
        }
        poolCache.clear();

        try {
            vertx.close().toCompletionStage().toCompletableFuture().join();
        } catch (Exception e) {
            LOG.warn("Failed to close Vert.x instance", e);
        }
    }

    // --- View tree walking ---

    /**
     * Recursively checks whether all sources in the view tree are SQL database sources from the same
     * database. Collects all {@link DatabaseSource} instances encountered.
     */
    private boolean allSourcesSameSqlDatabase(
            LogicalView view, Set<LogicalView> visited, Set<DatabaseSource> databaseSources) {
        if (!visited.add(view)) {
            return true;
        }

        if (!isViewOnCompatible(view.getViewOn(), visited, databaseSources)) {
            return false;
        }

        if (!allJoinParentViewsCompatible(view.getLeftJoins(), visited, databaseSources)) {
            return false;
        }

        return allJoinParentViewsCompatible(view.getInnerJoins(), visited, databaseSources);
    }

    private boolean isViewOnCompatible(
            AbstractLogicalSource viewOn, Set<LogicalView> visited, Set<DatabaseSource> databaseSources) {
        if (viewOn == null) {
            return false;
        }

        if (viewOn instanceof LogicalSource logicalSource) {
            return isLogicalSourceCompatible(logicalSource, databaseSources);
        }

        if (viewOn instanceof LogicalView nestedView) {
            return allSourcesSameSqlDatabase(nestedView, visited, databaseSources);
        }

        return false;
    }

    private boolean isLogicalSourceCompatible(LogicalSource logicalSource, Set<DatabaseSource> databaseSources) {
        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            return false;
        }

        var refIri = refFormulation.getAsResource();
        if (!SQL_FORMULATIONS.contains(refIri)) {
            return false;
        }

        var source = logicalSource.getSource();
        if (!(source instanceof DatabaseSource dbSource)) {
            return false;
        }

        // Check that this source is from the same database as all previously seen sources
        if (!databaseSources.isEmpty()) {
            var existingIdentity =
                    DatabaseSourceIdentity.of(databaseSources.iterator().next());
            var thisIdentity = DatabaseSourceIdentity.of(dbSource);
            if (!existingIdentity.equals(thisIdentity)) {
                LOG.debug(
                        "View mixes different databases: {} vs {}", existingIdentity.jdbcDsn(), thisIdentity.jdbcDsn());
                return false;
            }
        }

        databaseSources.add(dbSource);
        return true;
    }

    private boolean allJoinParentViewsCompatible(
            Set<LogicalViewJoin> joins, Set<LogicalView> visited, Set<DatabaseSource> databaseSources) {
        return joins.stream()
                .allMatch(join -> allSourcesSameSqlDatabase(join.getParentLogicalView(), visited, databaseSources));
    }

    private Optional<SqlClientProvider> findProvider(DatabaseSource source) {
        return providers.stream().filter(provider -> provider.supports(source)).findFirst();
    }
}
