package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.quotedName;

import io.carml.model.DatabaseSource;
import io.carml.model.Field;
import io.carml.model.LogicalSource;
import io.carml.vocab.Rdf;
import java.util.Set;
import org.eclipse.rdf4j.model.Resource;

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
 * <p>All fields are accessed via {@link ColumnSourceStrategy} (direct column references).
 */
final class SqlSourceHandler implements DuckDbSourceHandler {

    private static final Set<Resource> SUPPORTED =
            Set.of(Rdf.Ql.Rdb, Rdf.Rml.Rdb, Rdf.Rml.SQL2008Table, Rdf.Rml.SQL2008Query);

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
        var query = logicalSource.getQuery();
        if (query != null && !query.isBlank()) {
            return columnSource("(%s)".formatted(query), cteAlias);
        }

        var tableName = logicalSource.getTableName();
        if (tableName != null && !tableName.isBlank()) {
            return columnSource(quotedName(tableName).toString(), cteAlias);
        }

        var source = logicalSource.getSource();
        if (source instanceof DatabaseSource dbSource
                && dbSource.getQuery() != null
                && !dbSource.getQuery().isBlank()) {
            return columnSource("(%s)".formatted(dbSource.getQuery()), cteAlias);
        }

        throw new IllegalArgumentException("SQL logical source has no query or table name defined");
    }

    private static CompiledSource columnSource(String sourceSql, String cteAlias) {
        return new CompiledSource(
                sourceSql, new ColumnSourceStrategy(cteAlias, ColumnSourceStrategy.TypeCompanionMode.SQL_TYPEOF));
    }
}
