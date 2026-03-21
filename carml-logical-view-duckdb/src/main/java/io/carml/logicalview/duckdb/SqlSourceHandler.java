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
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

/**
 * Handles SQL database sources (RDB, SQL2008Table, SQL2008Query).
 *
 * <p>Supports three source modes:
 * <ul>
 *   <li>Inline query ({@code rml:query}): wrapped as a subquery</li>
 *   <li>Table name ({@code rr:tableName}): quoted table reference</li>
 *   <li>{@link DatabaseSource} with a query: wrapped as a subquery</li>
 * </ul>
 *
 * <p>For SQL2008Query sources backed by a {@link DatabaseSource}, the query is parsed from the
 * source database's SQL dialect (determined from the JDBC driver class) and re-rendered in DuckDB's
 * dialect. This translates dialect-specific syntax such as MySQL backtick-quoted identifiers
 * ({@code `column`} → {@code "column"}) and strips trailing semicolons.
 *
 * <p>All fields are accessed via {@link ColumnSourceStrategy} (direct column references).
 */
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

        var query = logicalSource.getQuery();
        if (query != null && !query.isBlank()) {
            return columnSource("(%s)".formatted(translateQuery(query, sourceDialect)), cteAlias);
        }

        var tableName = logicalSource.getTableName();
        if (tableName != null && !tableName.isBlank()) {
            return columnSource(quotedName(tableName).toString(), cteAlias);
        }

        var source = logicalSource.getSource();
        if (source instanceof DatabaseSource dbSource
                && dbSource.getQuery() != null
                && !dbSource.getQuery().isBlank()) {
            return columnSource("(%s)".formatted(translateQuery(dbSource.getQuery(), sourceDialect)), cteAlias);
        }

        throw new IllegalArgumentException("SQL logical source has no query or table name defined");
    }

    /**
     * Translates a SQL query from the source database dialect to DuckDB's dialect using jOOQ's
     * parser. Handles dialect-specific syntax such as MySQL backtick-quoted identifiers and trailing
     * semicolons. Falls back to the original query if parsing fails.
     */
    private static String translateQuery(String query, SQLDialect sourceDialect) {
        if (sourceDialect == SQLDialect.DUCKDB) {
            return query;
        }

        try {
            var cleanedQuery = query.strip().replaceAll(";\\s*$", "");
            var parsed = DSL.using(sourceDialect).parser().parseQuery(cleanedQuery);
            var settings = new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS);
            var translated = DSL.using(SQLDialect.DUCKDB, settings).render(parsed);
            LOG.debug("Translated SQL from {} to DuckDB: {} → {}", sourceDialect, query, translated);
            return translated;
        } catch (org.jooq.exception.DataAccessException e) {
            LOG.warn("Failed to translate SQL from {}, using original query: {}", sourceDialect, e.getMessage());
            return query;
        }
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

    private static CompiledSource columnSource(String sourceSql, String cteAlias) {
        return new CompiledSource(
                sourceSql, new ColumnSourceStrategy(cteAlias, ColumnSourceStrategy.TypeCompanionMode.SQL_TYPEOF));
    }
}
