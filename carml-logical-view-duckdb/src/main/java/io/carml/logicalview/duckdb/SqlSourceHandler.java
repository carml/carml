package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.quotedName;

import io.carml.model.DatabaseSource;
import io.carml.model.Field;
import io.carml.model.LogicalSource;
import io.carml.vocab.Rdf;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Resource;
import org.jooq.SQLDialect;
import org.jooq.conf.MappedCatalog;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.ParseUnknownFunctions;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

/**
 * Handles SQL database sources (RDB, SQL2008Table, SQL2008Query).
 *
 * <p>Supports three source modes:
 * <ul>
 *   <li>Table name ({@code rr:tableName} / {@code rml:SQL2008Table}): fully qualified table
 *       reference when a {@link DuckDbDatabaseAttacher} is available</li>
 *   <li>Inline query ({@code rml:query} / {@code rml:SQL2008Query}): parsed, table references
 *       qualified via jOOQ {@link RenderMapping}, and wrapped as a subquery</li>
 *   <li>{@link DatabaseSource} with a query: same as inline query</li>
 * </ul>
 *
 * <p>When a {@link DuckDbDatabaseAttacher} is provided and the logical source references a
 * {@link DatabaseSource} with a JDBC DSN, the handler ATTACHes the remote database (if not already
 * ATTACHed) and uses the resulting catalog and schema to produce fully qualified table references.
 * This eliminates the need for {@code USE <catalog>.<schema>} on duplicated connections.
 *
 * <p>For SQL2008Query sources backed by a {@link DatabaseSource}, the query is parsed from the
 * source database's SQL dialect (determined from the JDBC driver class) and re-rendered in DuckDB's
 * dialect. This translates dialect-specific syntax such as MySQL backtick-quoted identifiers
 * ({@code `column`} &rarr; {@code "column"}) and strips trailing semicolons. Unqualified table
 * references are mapped to the ATTACHed catalog and schema via jOOQ's {@link RenderMapping}.
 *
 * <p>All fields are accessed via {@link ColumnSourceStrategy} (direct column references).
 */
@SuppressWarnings("UnstableApiUsage")
@Slf4j
final class SqlSourceHandler implements DuckDbSourceHandler {

    private static final Set<Resource> SUPPORTED =
            Set.of(Rdf.Ql.Rdb, Rdf.Rml.Rdb, Rdf.Rml.SQL2008Table, Rdf.Rml.SQL2008Query);

    private static final Map<String, SQLDialect> DRIVER_TO_DIALECT = Map.of(
            "com.mysql.cj.jdbc.Driver", SQLDialect.MYSQL,
            "com.mysql.jdbc.Driver", SQLDialect.MYSQL,
            "org.postgresql.Driver", SQLDialect.POSTGRES,
            "com.microsoft.sqlserver.jdbc.SQLServerDriver", SQLDialect.DEFAULT);

    @Override
    public Set<Resource> supportedFormulations() {
        return SUPPORTED;
    }

    @Override
    public boolean isCompatible(LogicalSource logicalSource) {
        return true;
    }

    @Override
    public CompiledSource compileSource(LogicalSource logicalSource, Set<Field> viewFields, String cteAlias) {
        var sourceDialect = resolveSourceDialect(logicalSource);
        var catalogSchema = resolveCatalogSchema(logicalSource, DuckDbViewCompiler.currentDatabaseAttacher());

        // Check tableName first so SQL2008Table sources use direct table references instead of
        // the auto-generated "SELECT * FROM <table>" query. This allows clean qualification
        // without query rewriting.
        var tableName = logicalSource.getTableName();
        if (tableName != null && !tableName.isBlank()) {
            var qualifiedName = catalogSchema != null
                    ? quotedName(catalogSchema.catalog(), catalogSchema.schema(), tableName)
                            .toString()
                    : quotedName(tableName).toString();
            return columnSource(qualifiedName, cteAlias, viewFields);
        }

        var query = logicalSource.getQuery();
        if (query != null && !query.isBlank()) {
            var translated = translateQuery(query, sourceDialect, catalogSchema);
            return columnSource("(%s)".formatted(translated), cteAlias, viewFields);
        }

        var source = logicalSource.getSource();
        if (source instanceof DatabaseSource dbSource
                && dbSource.getQuery() != null
                && !dbSource.getQuery().isBlank()) {
            var translated = translateQuery(dbSource.getQuery(), sourceDialect, catalogSchema);
            return columnSource("(%s)".formatted(translated), cteAlias, viewFields);
        }

        throw new IllegalArgumentException("SQL logical source has no query or table name defined");
    }

    /**
     * Resolves the {@link DuckDbDatabaseAttacher.CatalogSchema} for the logical source's database,
     * ATTACHing it if necessary. Returns {@code null} if no attacher is available or the source has
     * no {@link DatabaseSource} with a JDBC DSN.
     */
    private static DuckDbDatabaseAttacher.CatalogSchema resolveCatalogSchema(
            LogicalSource logicalSource, DuckDbDatabaseAttacher databaseAttacher) {
        if (databaseAttacher == null) {
            return null;
        }

        var source = logicalSource.getSource();
        if (source instanceof DatabaseSource dbSource
                && dbSource.getJdbcDsn() != null
                && !dbSource.getJdbcDsn().isBlank()) {
            return databaseAttacher.attachIfNeeded(dbSource);
        }

        return null;
    }

    /**
     * Translates a SQL query from the source database dialect to DuckDB's dialect using jOOQ's
     * parser. Handles dialect-specific syntax such as MySQL backtick-quoted identifiers and trailing
     * semicolons. When a catalog/schema is provided, unqualified table references in the parsed query
     * are qualified with the ATTACHed database's catalog and schema via jOOQ's {@link RenderMapping}.
     *
     * <p>The mapping uses {@link MappedCatalog} and {@link MappedSchema} to map tables without an
     * explicit catalog/schema to the ATTACHed database's catalog and schema. This ensures that
     * queries like {@code SELECT * FROM student} render as
     * {@code SELECT * FROM "<catalog>"."<schema>"."student"}.
     *
     * <p>Falls back to the original query if parsing fails.
     */
    private static String translateQuery(
            String query, SQLDialect sourceDialect, DuckDbDatabaseAttacher.CatalogSchema catalogSchema) {
        // When the source dialect is DuckDB and no catalog/schema mapping is needed, pass the query
        // through verbatim to avoid unnecessary re-parsing and identifier quoting.
        if (sourceDialect == SQLDialect.DUCKDB && catalogSchema == null) {
            return query;
        }

        var cleanedQuery = query.strip().replaceAll(";\\s*$", "");

        // Try parsing with the source dialect first, then fall back to DEFAULT for more lenient
        // parsing (e.g., when the source dialect doesn't recognize database-specific functions).
        var parseDialects = sourceDialect == SQLDialect.DEFAULT || sourceDialect == SQLDialect.DUCKDB
                ? new SQLDialect[] {SQLDialect.DEFAULT}
                : new SQLDialect[] {sourceDialect, SQLDialect.DEFAULT};

        for (var parseDialect : parseDialects) {
            try {
                var parseSettings = new Settings().withParseUnknownFunctions(ParseUnknownFunctions.IGNORE);
                var parsed = DSL.using(parseDialect, parseSettings).parser().parseQuery(cleanedQuery);

                var settings = new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS);

                // When a catalog/schema is available, use RenderMapping to qualify unqualified table
                // references. The MappedCatalog maps tables with no catalog (input="") to the
                // ATTACHed catalog, and the nested MappedSchema maps tables with no schema
                // (input="") to the ATTACHed schema.
                if (catalogSchema != null) {
                    settings.withRenderCatalog(true)
                            .withRenderSchema(true)
                            .withRenderMapping(new RenderMapping()
                                    .withCatalogs(new MappedCatalog()
                                            .withInput("")
                                            .withOutput(catalogSchema.catalog())
                                            .withSchemata(new MappedSchema()
                                                    .withInput("")
                                                    .withOutput(catalogSchema.schema()))));
                }

                var translated = DSL.using(SQLDialect.DUCKDB, settings).render(parsed);
                LOG.debug("Translated SQL from {} to DuckDB: {} -> {}", parseDialect, query, translated);
                return translated;
            } catch (org.jooq.exception.DataAccessException e) {
                LOG.debug(
                        "Failed to parse SQL with {} dialect, {}: {}",
                        parseDialect,
                        parseDialect == parseDialects[parseDialects.length - 1] ? "giving up" : "retrying",
                        e.getMessage());
            }
        }

        LOG.warn("Failed to translate SQL query, using original: {}", query);
        return query;
    }

    private static SQLDialect resolveSourceDialect(LogicalSource logicalSource) {
        var source = logicalSource.getSource();
        if (source instanceof DatabaseSource dbSource && dbSource.getJdbcDriver() != null) {
            var dialect = DRIVER_TO_DIALECT.get(dbSource.getJdbcDriver());
            if (dialect != null) {
                return dialect;
            }
        }
        return SQLDialect.DUCKDB;
    }

    private static CompiledSource columnSource(String sourceSql, String cteAlias, Set<Field> viewFields) {
        return new CompiledSource(
                sourceSql,
                ColumnSourceStrategy.create(viewFields, cteAlias, ColumnSourceStrategy.TypeCompanionMode.SQL_TYPEOF));
    }
}
