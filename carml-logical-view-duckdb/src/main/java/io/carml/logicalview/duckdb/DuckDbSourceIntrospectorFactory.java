package io.carml.logicalview.duckdb;

import com.google.auto.service.AutoService;
import io.carml.logicalsourceresolver.MatchedLogicalSourceResolverFactory.MatchScore;
import io.carml.logicalview.MatchedSourceIntrospector;
import io.carml.logicalview.SourceIntrospectorFactory;
import io.carml.model.LogicalSource;
import io.carml.vocab.Rdf;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;

/**
 * A {@link SourceIntrospectorFactory} that produces {@link DuckDbSourceIntrospector} instances for
 * DuckDB-compatible data sources. Discovered via {@link java.util.ServiceLoader}.
 *
 * <p>Matches the same set of reference formulations as
 * {@link DuckDbLogicalViewEvaluatorFactory}: JSON (JsonPath), CSV, and SQL database sources
 * (RDB, SQL2008Table, SQL2008Query). Returns a strong match score to take priority over
 * generic introspectors.
 *
 * <p>This factory manages its own DuckDB JDBC connection lifecycle. When created via the
 * zero-arg constructor (ServiceLoader), an in-memory DuckDB connection is created. The factory
 * implements {@link AutoCloseable} to allow explicit cleanup.
 */
@Slf4j
@AutoService(SourceIntrospectorFactory.class)
public class DuckDbSourceIntrospectorFactory implements SourceIntrospectorFactory, AutoCloseable {

    private static final Set<Resource> COMPATIBLE_REF_FORMULATIONS = Set.of(
            Rdf.Ql.JsonPath,
            Rdf.Rml.JsonPath,
            Rdf.Ql.Csv,
            Rdf.Rml.Csv,
            Rdf.Ql.Rdb,
            Rdf.Rml.Rdb,
            Rdf.Rml.SQL2008Table,
            Rdf.Rml.SQL2008Query);

    private static final MatchScore STRONG_MATCH =
            MatchScore.builder().strongMatch().build();

    private final Connection connection;

    /**
     * Creates a factory with an in-memory DuckDB connection. Used by {@link java.util.ServiceLoader}
     * for automatic discovery.
     */
    public DuckDbSourceIntrospectorFactory() {
        this(createInMemoryConnection());
    }

    /**
     * Creates a factory with the given DuckDB JDBC connection.
     *
     * @param connection the DuckDB JDBC connection to use for introspection
     */
    public DuckDbSourceIntrospectorFactory(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Optional<MatchedSourceIntrospector> match(LogicalSource logicalSource) {
        var refFormulation = logicalSource.getReferenceFormulation();
        if (refFormulation == null) {
            LOG.debug("LogicalSource has no reference formulation");
            return Optional.empty();
        }

        var refIri = refFormulation.getAsResource();
        if (!COMPATIBLE_REF_FORMULATIONS.contains(refIri)) {
            LOG.debug("Reference formulation [{}] is not compatible with DuckDB introspection", refIri);
            return Optional.empty();
        }

        LOG.debug("LogicalSource matched for DuckDB introspection (ref formulation: {})", refIri);
        var introspector = new DuckDbSourceIntrospector(connection);
        return Optional.of(MatchedSourceIntrospector.of(STRONG_MATCH, introspector));
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

    private static Connection createInMemoryConnection() {
        try {
            return DriverManager.getConnection("jdbc:duckdb:");
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to create in-memory DuckDB connection. Ensure DuckDB JDBC is on the classpath.", e);
        }
    }
}
