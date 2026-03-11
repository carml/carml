package io.carml.logicalview.duckdb;

import static org.jooq.impl.DSL.castNull;
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
 *
 * @param cteAlias the alias of the CTE containing the source data
 * @param hasTypeCompanions {@code true} if the upstream source produces {@code .__type} companion
 *     columns (e.g. from a compiled inner view). When {@code false} (e.g. CSV, SQL, or
 *     {@code read_json_auto} sources), type companions are compiled as {@code CAST(NULL AS
 *     VARCHAR)}.
 */
record ColumnSourceStrategy(String cteAlias, boolean hasTypeCompanions) implements DuckDbSourceStrategy {

    @Override
    public boolean isMultiValuedReference(String reference) {
        return false;
    }

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
    public SelectField<?> compileFieldTypeReference(String reference, Name typeAlias) {
        if (hasTypeCompanions) {
            // View-on-view: project the type companion from the inner compiled view
            return field(quotedName(cteAlias, reference + TYPE_SUFFIX)).as(typeAlias);
        }
        // CSV/SQL/read_json_auto: no JSON type info available
        return castNull(String.class).as(typeAlias);
    }

    @Override
    public SelectField<?> compileNestedFieldTypeReference(String unnestAlias, String reference, Name typeAlias) {
        if (hasTypeCompanions) {
            return field(quotedName(unnestAlias, reference + TYPE_SUFFIX)).as(typeAlias);
        }
        return castNull(String.class).as(typeAlias);
    }

    @Override
    public Field<Object> resolveJoinChildReference(String childRef) {
        return field(quotedName(cteAlias, childRef));
    }
}
