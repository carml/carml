package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.quotedName;
import static org.jooq.impl.DSL.table;

import org.jooq.Field;
import org.jooq.Name;
import org.jooq.SelectField;
import org.jooq.Table;

/**
 * Source strategy for column-based sources where field references map directly to table columns.
 *
 * <p>All field references are compiled as direct column references qualified by the CTE alias.
 */
record ColumnSourceStrategy(String cteAlias) implements DuckDbSourceStrategy {

    @Override
    public SelectField<?> compileFieldReference(String reference, Name fieldAlias) {
        return field(quotedName(cteAlias, reference)).as(fieldAlias);
    }

    @Override
    public Field<?> compileTemplateReference(String segmentValue) {
        return field(quotedName(cteAlias, segmentValue));
    }

    @Override
    public SelectField<?> compileNestedFieldReference(String unnestAlias, String reference, Name fieldAlias) {
        return field(quotedName(unnestAlias, reference)).as(fieldAlias);
    }

    @Override
    public Table<?> compileUnnestTable(String iterator, String parentAlias, boolean isRootLevel, String absoluteName) {
        // Uses LATERAL subquery with parallel unnest to produce per-parent 0-based ordinals.
        // DuckDB 1.x does not support multi-argument unnest in FROM clause, but parallel
        // unnest works in SELECT clause within a LATERAL subquery.
        return table(
                        "LATERAL (SELECT unnest({0}) AS \"unnest\", unnest(range(len({0}))) AS \"" + ORDINAL_FIELD
                                + "\")",
                        field(quotedName(parentAlias, iterator)))
                .as(quotedName(absoluteName));
    }

    @Override
    public Field<Object> resolveJoinChildReference(String childRef) {
        return field(quotedName(cteAlias, childRef));
    }
}
